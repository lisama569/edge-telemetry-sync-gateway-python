"""设备注册与凭据管理。

三类鉴权失败必须可明确区分：
- 凭据不存在（设备未注册 / secret 错误）→ CREDENTIAL_NOT_FOUND
- 凭据过期                             → CREDENTIAL_EXPIRED
- 作用范围不足（越权）                 → SCOPE_DENIED
判定顺序刻意固定：先存在性、再有效期、最后作用范围。
"""

from __future__ import annotations

import secrets

from edge_telemetry.errors import RejectReason, TelemetryError
from edge_telemetry.models import (
    DeviceCredential,
    utcnow,
)
from edge_telemetry.storage import LocalStore


class CredentialManager:
    def __init__(self, store: LocalStore) -> None:
        self._store = store

    def register_device(
        self,
        device_id: str,
        scopes: frozenset[str],
        ttl_seconds: int,
    ) -> DeviceCredential:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds 必须为正整数")
        from datetime import timedelta

        now = utcnow()
        cred = DeviceCredential(
            device_id=device_id,
            secret=secrets.token_hex(24),
            scopes=frozenset(scopes),
            issued_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        self._store.upsert_credential(cred)
        return cred

    def get_credential(self, device_id: str) -> DeviceCredential | None:
        return self._store.get_credential(device_id)

    def authenticate(
        self, device_id: str, secret: str, required_scope: str
    ) -> DeviceCredential:
        cred = self._store.get_credential(device_id)
        # 1) 凭据不存在或密钥不匹配：统一归为"凭据不存在"，避免设备存在性泄漏。
        if cred is None or not secrets.compare_digest(cred.secret, secret):
            raise TelemetryError(
                RejectReason.CREDENTIAL_NOT_FOUND,
                f"设备 {device_id} 的凭据不存在或密钥不匹配",
            )
        # 2) 有效期检查。
        if cred.is_expired():
            raise TelemetryError(
                RejectReason.CREDENTIAL_EXPIRED,
                f"设备 {device_id} 凭据已于 {cred.expires_at.isoformat()} 过期",
            )
        # 3) 作用范围检查。
        if not cred.has_scope(required_scope):
            raise TelemetryError(
                RejectReason.SCOPE_DENIED,
                f"设备 {device_id} 凭据作用范围 {sorted(cred.scopes)} "
                f"不包含 {required_scope}",
            )
        return cred
