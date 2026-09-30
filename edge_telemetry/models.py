"""核心数据模型（仅类型定义，无业务逻辑）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

# 凭据内置作用范围：telemetry:write 允许上报遥测；
# 设备注册凭据可携带 register:device。
SCOPE_TELEMETRY_WRITE = "telemetry:write"
SCOPE_REGISTER_DEVICE = "register:device"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class DeviceCredential:
    device_id: str
    secret: str
    scopes: frozenset[str]
    expires_at: datetime
    issued_at: datetime = field(default_factory=utcnow)

    def is_expired(self, now: datetime | None = None) -> bool:
        current = now or utcnow()
        return current >= self.expires_at

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes


@dataclass(frozen=True)
class TelemetryBatch:
    """一次批量遥测上报。

    - ``batch_id`` 由设备生成，用于幂等去重；
    - ``seq`` 为设备维度单调递增序号，乱序到达时据此排序/延迟接受；
    - ``payload`` 为遥测数据点列表，内容参与冲突指纹计算。
    """

    device_id: str
    batch_id: str
    seq: int
    payload: list[dict]
    produced_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class IngestResult:
    """云端对单个批次的判定结果。

    status 取值：
    - accepted：首次按序接受；
    - duplicate：重复批次（batch_id 相同且指纹一致），幂等返回成功但不重复落库；
    - deferred：序号超前（乱序），先持久化挂起，等缺口补齐后自动转为 accepted；
    - rejected：被拒绝，``reason`` 给出可区分的失败原因。
    """

    device_id: str
    batch_id: str
    seq: int
    status: str
    reason: str | None = None
    detail: str = ""

    @property
    def accepted(self) -> bool:
        return self.status in ("accepted", "duplicate")

    @property
    def duplicate(self) -> bool:
        return self.status == "duplicate"
