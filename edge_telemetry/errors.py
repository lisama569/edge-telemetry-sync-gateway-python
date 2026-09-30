"""失败原因定义。

所有被拒绝的上报都必须返回这里定义的、彼此可区分的原因码，
测试与 API 响应均使用 :class:`RejectReason` 判定失败类型。
"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """上报/同步被拒绝的明确原因。"""

    CREDENTIAL_NOT_FOUND = "credential_not_found"
    """凭据不存在（设备未注册或密钥错误）。"""

    CREDENTIAL_EXPIRED = "credential_expired"
    """凭据存在但已过有效期。"""

    SCOPE_DENIED = "scope_denied"
    """凭据有效但作用范围不包含本次上报操作。"""

    SEQUENCE_CONFLICT = "sequence_conflict"
    """同一设备同一序号已被不同内容的批次占用。"""

    BUFFER_FULL = "buffer_full"
    """边缘缓冲达到容量上限，按淘汰策略拒绝本次写入。"""


class TelemetryError(Exception):
    """携带明确 :class:`RejectReason` 的业务异常。"""

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")
