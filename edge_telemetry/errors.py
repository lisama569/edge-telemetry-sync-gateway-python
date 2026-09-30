"""错误类型与可区分的失败原因。

所有接入失败都通过 :class:`IngestionError` 表达，``code`` 是机器可判别的
稳定失败原因，``message`` 面向日志与人工排查。
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """接入 / 同步失败原因。

    取值刻意彼此独立，调用方必须能明确区分以下四类凭据与越权失败：
    CREDENTIAL_NOT_FOUND / CREDENTIAL_EXPIRED / SCOPE_DENIED / DEVICE_NOT_FOUND。
    """

    # 设备 / 凭据
    DEVICE_NOT_FOUND = "DEVICE_NOT_FOUND"
    CREDENTIAL_NOT_FOUND = "CREDENTIAL_NOT_FOUND"
    CREDENTIAL_EXPIRED = "CREDENTIAL_EXPIRED"
    SCOPE_DENIED = "SCOPE_DENIED"
    # 批次与序号
    SEQUENCE_CONFLICT = "SEQUENCE_CONFLICT"
    REORDER_WINDOW_FULL = "REORDER_WINDOW_FULL"
    CONTENT_HASH_MISMATCH = "CONTENT_HASH_MISMATCH"
    # 边缘缓冲
    BUFFER_FULL = "BUFFER_FULL"
    # 其它
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    TRANSPORT_ERROR = "TRANSPORT_ERROR"


class IngestionError(Exception):
    """携带稳定 :class:`ErrorCode` 的接入异常。"""

    def __init__(self, code: ErrorCode, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details: dict = details or {}

    def __str__(self) -> str:  # pragma: no cover - 调试辅助
        return f"{self.code.value}: {self.message}"
