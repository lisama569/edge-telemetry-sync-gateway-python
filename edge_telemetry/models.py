"""领域模型与稳定序列化（规范 JSON / 内容哈希）。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


def canonical_json(payload: Any) -> str:
    """稳定序列化：键排序、空白确定，供内容指纹与持久化使用。"""
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def content_hash(payload: Any) -> str:
    """遥测负载的 SHA-256 内容指纹。"""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


class CredentialScope(str, Enum):
    """凭据作用范围。"""

    TELEMETRY_INGEST = "telemetry:ingest"
    DEVICE_MANAGE = "device:manage"
    STATE_READ = "state:read"


class BatchStatus(str, Enum):
    """批次在云端的生命周期状态。"""

    APPLIED = "APPLIED"        # 已按序合并进设备状态
    DEFERRED = "DEFERRED"      # 乱序到达，等待缺口补齐
    REJECTED = "REJECTED"      # 冲突 / 非法，终态


class OutboxStatus(str, Enum):
    """边缘出站缓冲（outbox）中批次的状态。"""

    PENDING = "PENDING"        # 尚未成功同步
    SYNCED = "SYNCED"          # 已被云端接受（含幂等重放命中）
    DEAD = "DEAD"              # 永久性失败（如凭据终结），放弃回放


class OverflowPolicy(str, Enum):
    """缓冲达到上限时的策略。"""

    REJECT_NEW = "REJECT_NEW"  # 拒绝新批次（默认，不丢数据）
    DROP_OLDEST = "DROP_OLDEST"  # 淘汰最旧 PENDING，事件可观测


@dataclass
class Device:
    device_id: str
    registered_at: float
    state: dict[str, Any] = field(default_factory=dict)
    applied_seq: int = 0  # 已连续合并的最大序号（水位线）


@dataclass
class Credential:
    credential_id: str
    device_id: str
    secret: str
    scopes: list[str]
    issued_at: float
    expires_at: float
    revoked: bool = False

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Credential":
        return cls(**d)


@dataclass
class TelemetryBatch:
    """一次遥测上报：一个批次 = 一个确定序号 + 不可变负载。"""

    device_id: str
    seq: int
    points: list[dict[str, Any]]
    batch_id: str = ""
    hash: str = ""  # 服务端填充 / 校验的内容指纹

    def normalized(self) -> "TelemetryBatch":
        """返回补齐 batch_id / 内容指纹的归一化副本。"""
        h = content_hash(self.points)
        bid = self.batch_id or f"{self.device_id}:{self.seq}:{h[:16]}"
        return TelemetryBatch(
            device_id=self.device_id,
            seq=self.seq,
            points=self.points,
            batch_id=bid,
            hash=h,
        )

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "TelemetryBatch":
        return cls(**d)


@dataclass
class BatchRecord:
    """云端保存的批次判定记录（可追溯）。"""

    batch_id: str
    device_id: str
    seq: int
    hash: str
    status: str
    received_at: float
    reason: str = ""  # 判定依据说明


@dataclass
class IngestResult:
    """云端接入的判定结果。"""

    status: str
    batch_id: str
    device_id: str
    seq: int
    applied_seq: int
    reason: str = ""
    duplicate: bool = False


@dataclass
class OutboxEntry:
    """边缘出站缓冲条目。"""

    batch_id: str
    device_id: str
    seq: int
    points: list[dict[str, Any]]
    hash: str
    created_at: float
    attempts: int = 0
    last_error: str = ""
    status: str = OutboxStatus.PENDING.value

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "OutboxEntry":
        return cls(**d)
