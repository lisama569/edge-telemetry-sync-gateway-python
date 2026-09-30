"""边缘侧代理：凭据校验、离线持久缓冲、容量保护与重连回放。

- 在线：先本地鉴权，再直传云端（云端再做一次鉴权 + 幂等判定）。
- 离线：先本地鉴权，通过后写入持久化 spool（SQLite，重启不丢）；
  重复批次（同 device_id + batch_id）幂等忽略。
- 缓冲容量策略：默认 reject-new —— 达到 capacity 后拒绝新批次并抛出
  BUFFER_FULL，绝不静默丢弃；溢出/拒绝次数计入 metrics，可观测。
- 回放：重连后按 (device_id, seq, 入队顺序) 确定性顺序发送；
  云端已通过其他途径收到的批次会返回 duplicate，同样从缓冲移除；
  鉴权类失败（过期/越权）不移除缓冲，记录为失败，等待凭据更新。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field

from edge_telemetry.credentials import CredentialManager
from edge_telemetry.errors import RejectReason, TelemetryError
from edge_telemetry.ingest import CloudIngestService
from edge_telemetry.models import (
    IngestResult,
    SCOPE_TELEMETRY_WRITE,
    TelemetryBatch,
)
from edge_telemetry.storage import EdgeSpool


@dataclass
class EdgeMetrics:
    submitted: int = 0
    forwarded: int = 0
    spooled: int = 0
    duplicate_offline: int = 0
    buffer_full: int = 0
    replayed: int = 0
    replay_failures: int = 0
    events: list[str] = field(default_factory=list)

    def log(self, event: str) -> None:
        self.events.append(event)


def _row_to_batch(row) -> TelemetryBatch:
    from datetime import datetime

    return TelemetryBatch(
        device_id=row["device_id"],
        batch_id=row["batch_id"],
        seq=row["seq"],
        payload=json.loads(row["payload"]),
        produced_at=datetime.fromisoformat(row["produced_at"]),
    )


class EdgeAgent:
    def __init__(
        self,
        spool: EdgeSpool,
        cloud: CloudIngestService,
        credentials: CredentialManager,
        online: bool = True,
    ) -> None:
        self._spool = spool
        self._cloud = cloud
        self._credentials = credentials
        self._online = online
        self._lock = threading.Lock()
        self.metrics = EdgeMetrics()

    @property
    def online(self) -> bool:
        return self._online

    def set_online(self, online: bool) -> None:
        self._online = online
        self.metrics.log(f"link {'online' if online else 'offline'}")

    # ---- 设备侧入口 ----

    def submit(
        self, batch: TelemetryBatch, secret: str
    ) -> IngestResult:
        """设备上报入口。``secret`` 为设备凭据密钥。"""
        with self._lock:
            self.metrics.submitted += 1
            # 无论在线离线，边缘侧先做本地凭据校验，失败原因可区分。
            self._credentials.authenticate(
                batch.device_id, secret, SCOPE_TELEMETRY_WRITE
            )

            if self._online:
                result = self._cloud.ingest(batch)
                self.metrics.forwarded += 1
                self.metrics.log(
                    f"forward device={batch.device_id} batch={batch.batch_id} "
                    f"seq={batch.seq} -> {result.status}: {result.detail}"
                )
                return result

            return self._spool_locked(batch)

    def _spool_locked(self, batch: TelemetryBatch) -> IngestResult:
        if self._spool.get(batch.device_id, batch.batch_id) is not None:
            self.metrics.duplicate_offline += 1
            self.metrics.log(
                f"offline-duplicate device={batch.device_id} "
                f"batch={batch.batch_id}，幂等忽略"
            )
            return IngestResult(
                batch.device_id,
                batch.batch_id,
                batch.seq,
                status="duplicate",
                detail="离线缓冲中已存在相同 batch_id，幂等忽略",
            )

        if self._spool.count_pending() >= self._spool.capacity:
            self.metrics.buffer_full += 1
            self.metrics.log(
                f"buffer-full device={batch.device_id} batch={batch.batch_id} "
                f"capacity={self._spool.capacity}，按 reject-new 策略拒绝"
            )
            raise TelemetryError(
                RejectReason.BUFFER_FULL,
                f"边缘缓冲已满（capacity={self._spool.capacity}），"
                "按 reject-new 策略拒绝本次上报",
            )

        self._spool.add(batch)
        self.metrics.spooled += 1
        self.metrics.log(
            f"spooled device={batch.device_id} batch={batch.batch_id} "
            f"seq={batch.seq} pending={self._spool.count_pending()}"
        )
        return IngestResult(
            batch.device_id,
            batch.batch_id,
            batch.seq,
            status="deferred",
            detail=f"离线已缓冲，待重连回放（pending={self._spool.count_pending()}）",
        )

    # ---- 重连回放 ----

    def replay(self) -> list[IngestResult]:
        """按确定性顺序回放全部缓冲，返回每条的云端判定。"""
        results: list[IngestResult] = []
        with self._lock:
            for row in self._spool.pending_ordered():
                spool_id = row["id"]
                batch = _row_to_batch(row)
                cred = self._credentials_authenticate_optional(
                    batch.device_id
                )
                if cred is None:
                    # 凭据过期/越权等：保留在缓冲，记录失败，跳过。
                    self.metrics.replay_failures += 1
                    continue
                result = self._cloud.ingest(batch)
                self.metrics.replayed += 1
                self.metrics.log(
                    f"replay device={batch.device_id} batch={batch.batch_id} "
                    f"seq={batch.seq} -> {result.status}: {result.detail}"
                )
                self._spool.remove(spool_id)
                results.append(result)
        return results

    def _credentials_authenticate_optional(self, device_id: str):
        """回放时无法持有原始 secret，直接读取已登记凭据判断有效性/范围。

        过期或范围缺失时返回 None（缓冲保留）；设备已删除同样返回 None。
        """
        cred = self._credentials.get_credential(device_id)
        if cred is None or cred.is_expired():
            return None
        if not cred.has_scope(SCOPE_TELEMETRY_WRITE):
            return None
        return cred
