"""云端接入网关：鉴权、幂等、乱序合并、冲突拒绝与可追溯记录。

序号与合并规则（确定性）
-----------------------
每个设备维护：

* ``applied_seq``：已连续并入设备状态的水位线（从 0 开始）；
* 批次账本 ``batch:<dev>:<batch_id>``：记录每个批次的内容指纹与判定结果；
* 乱序缓冲 ``pending:<dev>``：序号大于 ``applied_seq + 1`` 的早到批次。

一个批次到达时：

1. 鉴权（见 :class:`~edge_telemetry.registry.DeviceRegistry`），失败原样抛出；
2. 校验内容指纹，防篡改 → ``CONTENT_HASH_MISMATCH``；
3. 序号必须 > applied_seq；序号已并入 → 幂等命中（内容必须与首次一致，
   否则 ``SEQUENCE_CONFLICT``）；
4. batch_id 已存在：同内容返回首次判定（幂等），不同内容 → ``SEQUENCE_CONFLICT``；
5. seq == applied_seq + 1：立即并入并级联提升水位线（填补缺口）；
6. seq > applied_seq + 1：进入乱序缓冲（``DEFERRED``）；缓冲满 →
   ``REORDER_WINDOW_FULL``；同一序号缓冲中已有不同内容 → ``SEQUENCE_CONFLICT``。

合并语义确定：把该批次所有测点 ``points`` 按列表顺序逐个
``state[key] = value`` 写入（同键后者覆盖），故相同输入序列必得相同状态。
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from .errors import ErrorCode, IngestionError
from .models import (
    BatchRecord,
    BatchStatus,
    CredentialScope,
    IngestResult,
    TelemetryBatch,
    content_hash,
)
from .registry import DeviceRegistry
from .storage import KVStore

_log = logging.getLogger("edge-telemetry.gateway")

_BATCH_PREFIX = "batch:"
_PENDING_PREFIX = "pending:"


class CloudGateway:
    def __init__(
        self,
        store: KVStore,
        registry: DeviceRegistry,
        reorder_window: int = 1024,
        clock: Callable[[], float] = time.time,
        logger: logging.Logger | None = None,
    ):
        self.store = store
        self.registry = registry
        self.reorder_window = reorder_window
        self.clock = clock
        self.logger = logger or _log
        # 同一设备并发上报串行化；不同设备可并行
        self._device_locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

    # ---- 内部辅助 ----

    def _lock_for(self, device_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._device_locks.get(device_id)
            if lock is None:
                lock = threading.RLock()
                self._device_locks[device_id] = lock
            return lock

    def _batch_key(self, device_id: str, batch_id: str) -> str:
        return f"{_BATCH_PREFIX}{device_id}:{batch_id}"

    def _pending_key(self, device_id: str) -> str:
        return _PENDING_PREFIX + device_id

    def _load_pending(self, device_id: str) -> dict[str, Any]:
        return self.store.get(self._pending_key(device_id), {})

    def _save_pending(self, device_id: str, pending: dict[str, Any]) -> None:
        self.store.put(self._pending_key(device_id), pending)

    def _record(self, rec: BatchRecord) -> None:
        self.store.put(
            self._batch_key(rec.device_id, rec.batch_id),
            {
                "batch_id": rec.batch_id,
                "device_id": rec.device_id,
                "seq": rec.seq,
                "hash": rec.hash,
                "status": rec.status,
                "received_at": rec.received_at,
                "reason": rec.reason,
            },
        )

    @staticmethod
    def _merge_points(state: dict, points: list) -> dict:
        for p in points:
            if not isinstance(p, dict):
                raise IngestionError(
                    ErrorCode.INVALID_PAYLOAD, "每个测点必须是 JSON 对象"
                )
            for key, value in p.items():
                state[key] = value
        return state

    def _promote_chain(self, device, pending: dict) -> list[int]:
        """从 applied_seq+1 开始，把连续序号级联合并，返回被提升的序号。"""
        promoted: list[int] = []
        expect = device.applied_seq + 1
        while str(expect) in pending:
            entry = pending.pop(str(expect))
            self._merge_points(device.state, entry["points"])
            device.applied_seq = expect
            rec = BatchRecord(
                batch_id=entry["batch_id"],
                device_id=device.device_id,
                seq=expect,
                hash=entry["hash"],
                status=BatchStatus.APPLIED.value,
                received_at=entry["buffered_at"],
                reason="缺口补齐后按序并入",
            )
            self._record(rec)
            promoted.append(expect)
            expect += 1
        return promoted

    # ---- 对外接口 ----

    def ingest(
        self, batch: TelemetryBatch, credential_id: str, secret: str
    ) -> IngestResult:
        # 0) 基础形状与归一化
        if not isinstance(batch.points, list):
            raise IngestionError(
                ErrorCode.INVALID_PAYLOAD, "points 必须是数组"
            )
        if not isinstance(batch.seq, int) or batch.seq <= 0:
            raise IngestionError(
                ErrorCode.INVALID_PAYLOAD, "seq 必须是正整数"
            )
        nb = batch.normalized()

        # 1) 鉴权（失败原因由 registry 明确区分）
        cred = self.registry.authenticate(
            credential_id, secret, CredentialScope.TELEMETRY_INGEST
        )
        # 凭据只能给所属设备上报
        if cred.device_id != nb.device_id:
            raise IngestionError(
                ErrorCode.SCOPE_DENIED,
                "凭据不属于目标设备，禁止跨设备上报",
                details={
                    "credential_device": cred.device_id,
                    "target_device": nb.device_id,
                },
            )

        with self._lock_for(nb.device_id):
            device = self.registry.get_device(nb.device_id)
            pending = self._load_pending(nb.device_id)

            # 2) 内容指纹校验
            if content_hash(nb.points) != nb.hash:
                raise IngestionError(
                    ErrorCode.CONTENT_HASH_MISMATCH, "内容指纹不一致"
                )

            # 3) 已并入水位线的序号：幂等或冲突
            if nb.seq <= device.applied_seq:
                rec_raw = self.store.get(self._batch_key(nb.device_id, nb.batch_id))
                if rec_raw is None:
                    # 不同 batch_id 但撞已应用序号 → 序号冲突
                    raise IngestionError(
                        ErrorCode.SEQUENCE_CONFLICT,
                        f"序号 {nb.seq} 已并入且 batch_id 不一致",
                        details={"seq": nb.seq, "applied_seq": device.applied_seq},
                    )
                if rec_raw["hash"] != nb.hash:
                    raise IngestionError(
                        ErrorCode.SEQUENCE_CONFLICT,
                        f"序号 {nb.seq} 内容冲突，拒绝静默覆盖",
                        details={"seq": nb.seq,
                                 "stored_hash": rec_raw["hash"],
                                 "incoming_hash": nb.hash},
                    )
                self.logger.info(
                    "幂等命中 device=%s seq=%s batch=%s 判定依据=相同批次重复上报",
                    nb.device_id, nb.seq, nb.batch_id,
                )
                return IngestResult(
                    status=BatchStatus.APPLIED.value,
                    batch_id=nb.batch_id,
                    device_id=nb.device_id,
                    seq=nb.seq,
                    applied_seq=device.applied_seq,
                    reason="相同批次重复上报，幂等返回首次结果",
                    duplicate=True,
                )

            # 4) batch_id 账本已存在（通常是乱序缓冲中的同一批次）
            existing = self.store.get(
                self._batch_key(nb.device_id, nb.batch_id)
            )
            if existing is not None:
                if existing["hash"] != nb.hash:
                    raise IngestionError(
                        ErrorCode.SEQUENCE_CONFLICT,
                        f"批次 {nb.batch_id} 内容冲突，拒绝静默覆盖",
                        details={"stored_hash": existing["hash"],
                                 "incoming_hash": nb.hash},
                    )
                return IngestResult(
                    status=existing["status"],
                    batch_id=nb.batch_id,
                    device_id=nb.device_id,
                    seq=nb.seq,
                    applied_seq=device.applied_seq,
                    reason="相同 batch_id 重复上报，幂等返回既有判定",
                    duplicate=True,
                )

            # 5) 乱序缓冲中同序号冲突检测
            same_seq = pending.get(str(nb.seq))
            if same_seq is not None:
                if same_seq["hash"] != nb.hash:
                    raise IngestionError(
                        ErrorCode.SEQUENCE_CONFLICT,
                        f"序号 {nb.seq} 存在内容不同的批次，拒绝覆盖",
                        details={"seq": nb.seq},
                    )
                # 同内容不同 batch_id：视为重复重传，返回缓冲批次判定
                return IngestResult(
                    status=BatchStatus.DEFERRED.value,
                    batch_id=same_seq["batch_id"],
                    device_id=nb.device_id,
                    seq=nb.seq,
                    applied_seq=device.applied_seq,
                    reason="同序号同内容重复到达（已在缓冲）",
                    duplicate=True,
                )

            now = self.clock()

            # 6) 恰好是下一个序号：立即并入 + 级联提升
            if nb.seq == device.applied_seq + 1:
                self._merge_points(device.state, nb.points)
                device.applied_seq = nb.seq
                self._record(BatchRecord(
                    batch_id=nb.batch_id, device_id=nb.device_id, seq=nb.seq,
                    hash=nb.hash, status=BatchStatus.APPLIED.value,
                    received_at=now, reason="按序到达直接并入",
                ))
                promoted = self._promote_chain(device, pending)
                self._save_pending(nb.device_id, pending)
                self.registry.save_device(device)
                reason = "按序并入" + (
                    f"，并级联提升 {promoted}" if promoted else ""
                )
                self.logger.info(
                    "接收 device=%s seq=%s applied=%s 判定依据=%s",
                    nb.device_id, nb.seq, device.applied_seq, reason,
                )
                return IngestResult(
                    status=BatchStatus.APPLIED.value,
                    batch_id=nb.batch_id,
                    device_id=nb.device_id,
                    seq=nb.seq,
                    applied_seq=device.applied_seq,
                    reason=reason,
                )

            # 7) 乱序早到 → 延迟接受（缓冲）
            if len(pending) >= self.reorder_window:
                raise IngestionError(
                    ErrorCode.REORDER_WINDOW_FULL,
                    f"设备 {nb.device_id} 乱序缓冲已满({self.reorder_window})",
                )
            pending[str(nb.seq)] = {
                "batch_id": nb.batch_id,
                "hash": nb.hash,
                "points": nb.points,
                "buffered_at": now,
            }
            self._save_pending(nb.device_id, pending)
            self._record(BatchRecord(
                batch_id=nb.batch_id, device_id=nb.device_id, seq=nb.seq,
                hash=nb.hash, status=BatchStatus.DEFERRED.value,
                received_at=now,
                reason=f"乱序早到，等待序号 {device.applied_seq + 1}",
            ))
            self.logger.info(
                "延迟接受 device=%s seq=%s applied=%s 判定依据=乱序早到，等缺口",
                nb.device_id, nb.seq, device.applied_seq,
            )
            return IngestResult(
                status=BatchStatus.DEFERRED.value,
                batch_id=nb.batch_id,
                device_id=nb.device_id,
                seq=nb.seq,
                applied_seq=device.applied_seq,
                reason=f"乱序早到，等待序号 {device.applied_seq + 1}",
            )

    def device_snapshot(self, device_id: str) -> dict[str, Any]:
        """供一致性校验的云端视图：水位线 + 状态 + 各批次账本。"""
        device = self.registry.get_device(device_id)
        ledger_prefix = f"{_BATCH_PREFIX}{device_id}:"
        ledger = {
            k[len(ledger_prefix):]: v
            for k, v in self.store.prefix(ledger_prefix).items()
        }
        return {
            "device_id": device.device_id,
            "applied_seq": device.applied_seq,
            "state": dict(device.state),
            "pending_seqs": sorted(
                int(s) for s in self._load_pending(device_id).keys()
            ),
            "ledger": ledger,
        }
