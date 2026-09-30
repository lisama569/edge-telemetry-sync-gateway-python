"""边缘代理：序号分配、离线缓冲、重启恢复、重连回放与一致性校验。

数据安全
--------
* 出站缓冲（outbox）每条条目独立持久化在 KV 中（``outbox:<dev>:<batch_id>``），
  ``produce`` 落盘后才返回，因此进程被杀死 / 重启后未同步数据不丢；
* 序号计数器 ``seq:<dev>`` 同样持久化，重启后继续单调递增，绝不复用序号。

缓冲上限与淘汰（可观测）
----------------------
``buffer_capacity`` 限制 PENDING 条目数；达到上限时按 ``overflow_policy``：

* ``REJECT_NEW``（默认）：抛 ``BUFFER_FULL``，不丢任何已有数据；
* ``DROP_OLDEST``：淘汰最旧的 PENDING 条目后再入队，并在
  ``stats()['events']`` 留下 ``buffer_overflow`` 事件（含被淘汰 batch_id）。

重连回放
--------
``flush`` 从最小序号开始依次发送（序号顺序提交）：云端幂等保证重放安全；
``APPLIED``/``DEFERRED`` 均视为已被云端接收而离开 outbox，``DUPLICATE`` 同理；
传输不可用 → 停止本轮；凭据类终结失败 → 条目标 DEAD 并记录事件。
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from .errors import ErrorCode, IngestionError
from .models import (
    IngestResult,
    OutboxEntry,
    OutboxStatus,
    OverflowPolicy,
    TelemetryBatch,
    content_hash,
)
from .registry import DeviceRegistry
from .storage import KVStore
from .transport import Transport

_log = logging.getLogger("edge-telemetry.edge")

_OUTBOX_PREFIX = "outbox:"
_SEQ_PREFIX = "seq:"
_STATS_PREFIX = "estats:"


class EdgeAgent:
    def __init__(
        self,
        device_id: str,
        store: KVStore,
        transport: Transport,
        registry: DeviceRegistry,
        credential_id: str,
        secret: str,
        buffer_capacity: int = 1000,
        overflow_policy: OverflowPolicy | str = OverflowPolicy.REJECT_NEW,
        clock: Callable[[], float] = time.time,
        logger: logging.Logger | None = None,
    ):
        self.device_id = device_id
        self.store = store
        self.transport = transport
        self.registry = registry
        self.credential_id = credential_id
        self.secret = secret
        self.buffer_capacity = buffer_capacity
        self.overflow_policy = (
            overflow_policy
            if isinstance(overflow_policy, OverflowPolicy)
            else OverflowPolicy(overflow_policy)
        )
        self.clock = clock
        self.logger = logger or _log
        self._lock = threading.RLock()

    # ---- key 辅助 ----

    @property
    def _outbox_prefix(self) -> str:
        return f"{_OUTBOX_PREFIX}{self.device_id}:"

    @property
    def _seq_key(self) -> str:
        return _SEQ_PREFIX + self.device_id

    @property
    def _stats_key(self) -> str:
        return _STATS_PREFIX + self.device_id

    def _load_stats(self) -> dict[str, Any]:
        return self.store.get(
            self._stats_key,
            {"produced": 0, "synced": 0, "duplicates": 0, "dead": 0,
             "dropped": 0, "flush_runs": 0, "transport_failures": 0,
             "events": []},
        )

    def _save_stats(self, stats: dict) -> None:
        self.store.put(self._stats_key, stats)

    def _event(self, stats: dict, kind: str, detail: dict) -> None:
        event = {"at": self.clock(), "kind": kind, **detail}
        stats["events"].append(event)
        self.logger.warning("边缘事件 %s %s", kind, detail)

    def _load_outbox(self, status: str | None = None) -> list[OutboxEntry]:
        raw = self.store.prefix(self._outbox_prefix)
        entries = [OutboxEntry.from_dict(v) for v in raw.values()]
        if status is not None:
            entries = [e for e in entries if e.status == status]
        entries.sort(key=lambda e: (e.seq, e.batch_id))
        return entries

    # ---- 生产 ----

    def produce(self, points: list[dict]) -> OutboxEntry:
        """生成一个带单调序号的批次并持久化到 outbox（离线可用）。"""
        if not isinstance(points, list) or not points:
            raise IngestionError(
                ErrorCode.INVALID_PAYLOAD, "points 必须是非空数组"
            )
        with self._lock:
            stats = self._load_stats()
            pending = self._load_outbox(OutboxStatus.PENDING.value)
            if len(pending) >= self.buffer_capacity:
                if self.overflow_policy is OverflowPolicy.REJECT_NEW:
                    self._event(stats, "buffer_overflow", {
                        "policy": OverflowPolicy.REJECT_NEW.value,
                        "capacity": self.buffer_capacity,
                        "action": "rejected_new",
                    })
                    self._save_stats(stats)
                    raise IngestionError(
                        ErrorCode.BUFFER_FULL,
                        f"边缘缓冲已满({self.buffer_capacity})，拒绝新批次",
                        details={"capacity": self.buffer_capacity,
                                 "pending": len(pending)},
                    )
                # DROP_OLDEST：淘汰最旧 PENDING
                victim = pending[0]
                self.store.delete(self._outbox_prefix + victim.batch_id)
                stats["dropped"] += 1
                self._event(stats, "buffer_overflow", {
                    "policy": OverflowPolicy.DROP_OLDEST.value,
                    "capacity": self.buffer_capacity,
                    "action": "dropped_oldest",
                    "evicted_batch_id": victim.batch_id,
                    "evicted_seq": victim.seq,
                })

            seq = int(self.store.get(self._seq_key, 0)) + 1
            self.store.put(self._seq_key, seq)
            batch = TelemetryBatch(
                device_id=self.device_id, seq=seq, points=points
            ).normalized()
            entry = OutboxEntry(
                batch_id=batch.batch_id,
                device_id=self.device_id,
                seq=seq,
                points=points,
                hash=batch.hash,
                created_at=self.clock(),
            )
            self.store.put(self._outbox_prefix + entry.batch_id, entry.to_dict())
            stats["produced"] += 1
            self._save_stats(stats)
            self.logger.info("入队 device=%s seq=%s batch=%s",
                             self.device_id, seq, entry.batch_id)
            return entry

    # ---- 回放 ----

    def flush(self) -> dict[str, Any]:
        """按序号顺序回放所有 PENDING；返回本轮结果摘要。"""
        with self._lock:
            stats = self._load_stats()
            stats["flush_runs"] += 1
            pending = self._load_outbox(OutboxStatus.PENDING.value)
            synced, duplicates, dead, failures = [], [], 0, 0

            for entry in pending:
                batch = TelemetryBatch(
                    device_id=entry.device_id, seq=entry.seq,
                    points=entry.points, batch_id=entry.batch_id,
                    hash=entry.hash,
                )
                try:
                    result = self.transport.send(
                        batch, self.credential_id, self.secret
                    )
                except IngestionError as e:
                    entry.attempts += 1
                    entry.last_error = e.code.value
                    if e.code in (
                        ErrorCode.CREDENTIAL_NOT_FOUND,
                        ErrorCode.CREDENTIAL_EXPIRED,
                        ErrorCode.SCOPE_DENIED,
                        ErrorCode.CONTENT_HASH_MISMATCH,
                        ErrorCode.SEQUENCE_CONFLICT,
                    ):
                        # 终结性失败：重试无意义，标记 DEAD 并保留可追溯
                        entry.status = OutboxStatus.DEAD.value
                        stats["dead"] += 1
                        dead += 1
                        self.store.put(
                            self._outbox_prefix + entry.batch_id,
                            entry.to_dict(),
                        )
                        self._event(stats, "terminal_sync_failure", {
                            "batch_id": entry.batch_id, "seq": entry.seq,
                            "code": e.code.value,
                        })
                    else:
                        # 传输 / 缓冲窗口等临时失败：停止本轮，保留 PENDING
                        stats["transport_failures"] += 1
                        failures += 1
                        self.store.put(
                            self._outbox_prefix + entry.batch_id,
                            entry.to_dict(),
                        )
                        self.logger.info(
                            "回放暂停 seq=%s 原因=%s（数据保留待重放）",
                            entry.seq, e.code.value,
                        )
                    break

                entry.attempts += 1
                entry.last_error = ""
                # APPLIED / DEFERRED / duplicate 都表示云端已接收并持久化
                entry.status = OutboxStatus.SYNCED.value
                self.store.put(
                    self._outbox_prefix + entry.batch_id, entry.to_dict()
                )
                stats["synced"] += 1
                synced.append(entry.batch_id)
                if result.duplicate:
                    stats["duplicates"] += 1
                    duplicates.append(entry.batch_id)
                self.logger.info(
                    "已同步 seq=%s status=%s duplicate=%s",
                    entry.seq, result.status, result.duplicate,
                )

            self._save_stats(stats)
            remaining = self._load_outbox(OutboxStatus.PENDING.value)
            return {
                "synced": synced,
                "duplicates": duplicates,
                "dead": dead,
                "transport_failures": failures,
                "pending_remaining": len(remaining),
                "stopped": failures > 0,
            }

    # ---- 一致性 ----

    def local_view(self) -> dict[str, Any]:
        """边缘侧已知的设备视图：对所有已 SYNCED 批次做同样的确定性合并。"""
        synced_entries = self._load_outbox(OutboxStatus.SYNCED.value)
        state: dict[str, Any] = {}
        for e in synced_entries:
            for p in e.points:
                for k, v in p.items():
                    state[k] = v
        applied_seq = max((e.seq for e in synced_entries), default=0)
        return {"device_id": self.device_id,
                "applied_seq": applied_seq, "state": state}

    def verify_consistency(self) -> dict[str, Any]:
        """比较边缘本地投影与云端快照（水位线与状态必须一致）。"""
        local = self.local_view()
        cloud = self.transport.gateway.device_snapshot(self.device_id)  # type: ignore[attr-defined]
        consistent = (
            local["applied_seq"] == cloud["applied_seq"]
            and local["state"] == cloud["state"]
            and cloud["pending_seqs"] == []
        )
        return {
            "consistent": consistent,
            "local": local,
            "cloud": {"applied_seq": cloud["applied_seq"],
                      "state": cloud["state"],
                      "pending_seqs": cloud["pending_seqs"]},
        }

    def stats(self) -> dict[str, Any]:
        with self._lock:
            stats = self._load_stats()
            stats["pending"] = len(
                self._load_outbox(OutboxStatus.PENDING.value)
            )
            stats["synced_entries"] = len(
                self._load_outbox(OutboxStatus.SYNCED.value)
            )
            stats["dead_entries"] = len(
                self._load_outbox(OutboxStatus.DEAD.value)
            )
            return stats
