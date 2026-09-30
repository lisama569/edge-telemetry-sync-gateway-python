"""云端批量遥测接入：幂等、乱序延迟接受、序号冲突检测。

序号规则（设备维度，起始为 1，连续递增）：
- seq == 水位（next_expected）：立即 accepted，并尝试把已挂起的连续序号提升；
- seq >  水位：乱序超前，持久化为 deferred 挂起，缺口补齐后自动转 accepted；
- seq <  水位：该序号必然已存在，落到"重复"或"冲突"分支。

幂等规则：
- (device_id, batch_id) 唯一：重复批次返回 duplicate，不重复落库；
- 同 batch_id 但内容指纹不同 / 同 seq 但批次或内容不同：一律 SEQUENCE_CONFLICT
  拒绝，绝不静默覆盖。

并发：每设备一把锁串行化判定 + SQLite 唯一约束兜底。
"""

from __future__ import annotations

import hashlib
import json
import threading

from edge_telemetry.errors import RejectReason, TelemetryError
from edge_telemetry.models import IngestResult, TelemetryBatch
from edge_telemetry.storage import LocalStore


def content_fingerprint(batch: TelemetryBatch) -> str:
    """对批次内容做确定性指纹（payload 规范化后 SHA-256）。"""
    canonical = json.dumps(
        batch.payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class CloudIngestService:
    def __init__(self, store: LocalStore) -> None:
        self._store = store
        self._device_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _device_lock(self, device_id: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._device_locks.get(device_id)
            if lock is None:
                lock = threading.Lock()
                self._device_locks[device_id] = lock
            return lock

    def ingest(self, batch: TelemetryBatch) -> IngestResult:
        if batch.seq < 1:
            raise TelemetryError(
                RejectReason.SEQUENCE_CONFLICT,
                f"序号必须 >= 1，收到 seq={batch.seq}",
            )
        fp = content_fingerprint(batch)
        lock = self._device_lock(batch.device_id)
        with lock:
            # 1) 幂等检查：batch_id 维度。
            by_batch = self._store.get_batch_by_batch_id(
                batch.device_id, batch.batch_id
            )
            if by_batch is not None:
                if by_batch["content_hash"] == fp:
                    return IngestResult(
                        batch.device_id,
                        batch.batch_id,
                        batch.seq,
                        status="duplicate",
                        detail=(
                            f"重复批次 batch_id={batch.batch_id} "
                            f"(seq={by_batch['seq']}, status={by_batch['status']})，"
                            "内容指纹一致，幂等忽略"
                        ),
                    )
                raise TelemetryError(
                    RejectReason.SEQUENCE_CONFLICT,
                    f"batch_id={batch.batch_id} 已存在但内容指纹冲突："
                    f"已存 {by_batch['content_hash'][:12]} vs 新报 {fp[:12]}",
                )

            # 2) 序号冲突检查：同序号槽位只能有一个批次。
            by_seq = self._store.get_batch_by_seq(batch.device_id, batch.seq)
            if by_seq is not None:
                raise TelemetryError(
                    RejectReason.SEQUENCE_CONFLICT,
                    f"设备 {batch.device_id} 的 seq={batch.seq} 已被 "
                    f"batch_id={by_seq['batch_id']} 占用 "
                    f"(指纹 {by_seq['content_hash'][:12]} vs {fp[:12]})，拒绝覆盖",
                )

            # 3) 按水位判定立即接受还是延迟挂起。
            nxt = self._store.get_next_expected(batch.device_id)
            if batch.seq > nxt:
                self._store.insert_batch(batch, fp, status="deferred")
                return IngestResult(
                    batch.device_id,
                    batch.batch_id,
                    batch.seq,
                    status="deferred",
                    detail=(
                        f"乱序超前：收到 seq={batch.seq}，期望 seq={nxt}，"
                        "已挂起等待缺口补齐"
                    ),
                )

            self._store.insert_batch(batch, fp, status="accepted")
            promoted = self._store.promote_due_batches(batch.device_id)
            detail = f"按序接受 seq={batch.seq}"
            if promoted:
                detail += f"；缺口补齐，连带提升 {promoted}"
            return IngestResult(
                batch.device_id,
                batch.batch_id,
                batch.seq,
                status="accepted",
                detail=detail,
            )
