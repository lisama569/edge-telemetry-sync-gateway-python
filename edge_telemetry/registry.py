"""设备注册与凭据管理（有效期、作用范围、明确区分的失败原因）。

鉴权失败原因刻意区分为四个独立 :class:`~edge_telemetry.errors.ErrorCode`：

1. ``CREDENTIAL_NOT_FOUND``：凭据不存在 / 已撤销 / 密钥不匹配；
2. ``CREDENTIAL_EXPIRED``：凭据存在但已过有效期；
3. ``SCOPE_DENIED``：凭据有效但不具备所需作用范围（越权）；
4. ``DEVICE_NOT_FOUND``：设备未注册。

判定顺序固定：凭据存在性 → 设备存在性 → 有效期 → 作用范围，
保证同一输入永远得到同一个失败原因。
"""

from __future__ import annotations

import secrets
import time
from dataclasses import asdict
from typing import Callable

from .errors import ErrorCode, IngestionError
from .models import Credential, CredentialScope, Device
from .storage import KVStore

_DEVICE_PREFIX = "device:"
_CRED_PREFIX = "credential:"


class DeviceRegistry:
    def __init__(self, store: KVStore, clock: Callable[[], float] = time.time):
        self.store = store
        self.clock = clock

    # ---- 设备 ----

    def register_device(self, device_id: str) -> Device:
        """幂等注册：重复注册返回既有设备。"""
        raw = self.store.get(_DEVICE_PREFIX + device_id)
        if raw is not None:
            return self._device_from_dict(raw)
        device = Device(device_id=device_id, registered_at=self.clock())
        self.store.put(
            _DEVICE_PREFIX + device_id,
            asdict(device),
        )
        return device

    def get_device(self, device_id: str) -> Device:
        raw = self.store.get(_DEVICE_PREFIX + device_id)
        if raw is None:
            raise IngestionError(
                ErrorCode.DEVICE_NOT_FOUND, f"设备不存在: {device_id}"
            )
        return self._device_from_dict(raw)

    def save_device(self, device: Device) -> None:
        self.store.put(_DEVICE_PREFIX + device.device_id, asdict(device))

    @staticmethod
    def _device_from_dict(raw: dict) -> Device:
        return Device(
            device_id=raw["device_id"],
            registered_at=raw["registered_at"],
            state=raw.get("state", {}),
            applied_seq=raw.get("applied_seq", 0),
        )

    # ---- 凭据 ----

    def issue_credential(
        self,
        device_id: str,
        scopes: list[CredentialScope] | list[str],
        ttl_seconds: float = 3600.0,
    ) -> Credential:
        # 设备必须已注册
        self.get_device(device_id)
        if ttl_seconds <= 0:
            raise IngestionError(
                ErrorCode.INVALID_PAYLOAD, "凭据有效期必须为正数"
            )
        now = self.clock()
        scope_values = [
            s.value if isinstance(s, CredentialScope) else str(s) for s in scopes
        ]
        cred = Credential(
            credential_id="cred-" + secrets.token_hex(8),
            device_id=device_id,
            secret=secrets.token_urlsafe(24),
            scopes=scope_values,
            issued_at=now,
            expires_at=now + ttl_seconds,
        )
        self.store.put(_CRED_PREFIX + cred.credential_id, cred.to_dict())
        return cred

    def revoke_credential(self, credential_id: str) -> None:
        raw = self.store.get(_CRED_PREFIX + credential_id)
        if raw is None:
            raise IngestionError(
                ErrorCode.CREDENTIAL_NOT_FOUND,
                f"凭据不存在: {credential_id}",
            )
        raw["revoked"] = True
        self.store.put(_CRED_PREFIX + credential_id, raw)

    def get_credential(self, credential_id: str) -> Credential | None:
        raw = self.store.get(_CRED_PREFIX + credential_id)
        return Credential.from_dict(raw) if raw is not None else None

    def authenticate(
        self,
        credential_id: str,
        secret: str,
        required_scope: CredentialScope,
    ) -> Credential:
        """按固定顺序鉴权，失败时抛出原因可区分的 :class:`IngestionError`。"""
        cred = self.get_credential(credential_id)
        # 1) 凭据不存在 / 已撤销 / 密钥错误
        if cred is None or cred.revoked or not secrets.compare_digest(
            cred.secret, secret
        ):
            raise IngestionError(
                ErrorCode.CREDENTIAL_NOT_FOUND,
                f"凭据不存在或无效: {credential_id}",
                details={"credential_id": credential_id},
            )
        # 2) 设备存在性
        try:
            self.get_device(cred.device_id)
        except IngestionError:
            raise IngestionError(
                ErrorCode.DEVICE_NOT_FOUND,
                f"凭据所属设备不存在: {cred.device_id}",
            )
        # 3) 有效期（到期时刻本身即视为过期）
        if self.clock() >= cred.expires_at:
            raise IngestionError(
                ErrorCode.CREDENTIAL_EXPIRED,
                f"凭据已过期: {credential_id}",
                details={"expires_at": cred.expires_at, "now": self.clock()},
            )
        # 4) 作用范围
        required = (
            required_scope.value
            if isinstance(required_scope, CredentialScope)
            else required_scope
        )
        if required not in cred.scopes:
            raise IngestionError(
                ErrorCode.SCOPE_DENIED,
                f"凭据缺少作用范围 {required}: {credential_id}",
                details={"required": required, "granted": list(cred.scopes)},
            )
        return cred
