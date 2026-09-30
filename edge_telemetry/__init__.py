"""边缘设备遥测接入与云端同步（本地模拟）。"""

from .errors import ErrorCode, IngestionError
from .models import (
    BatchStatus,
    Credential,
    CredentialScope,
    IngestResult,
    OutboxEntry,
    OutboxStatus,
    OverflowPolicy,
    TelemetryBatch,
)

__all__ = [
    "ErrorCode",
    "IngestionError",
    "BatchStatus",
    "Credential",
    "CredentialScope",
    "IngestResult",
    "OutboxEntry",
    "OutboxStatus",
    "OverflowPolicy",
    "TelemetryBatch",
]
