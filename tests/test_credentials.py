"""凭据：不存在 / 过期 / 越权三类失败必须可明确区分。"""

from __future__ import annotations

from datetime import timedelta

import pytest

from edge_telemetry.errors import RejectReason, TelemetryError
from edge_telemetry.models import SCOPE_REGISTER_DEVICE, SCOPE_TELEMETRY_WRITE, utcnow
from tests.conftest import case_log


def test_credential_not_found_device_unknown(credentials):
    case_log("凭据不存在-未注册设备", device_id="ghost", secret="x")
    with pytest.raises(TelemetryError) as ei:
        credentials.authenticate("ghost", "x", SCOPE_TELEMETRY_WRITE)
    case_log("判定", reason=ei.value.reason.value, detail=ei.value.detail)
    assert ei.value.reason is RejectReason.CREDENTIAL_NOT_FOUND


def test_credential_not_found_wrong_secret(credentials):
    cred = credentials.register_device(
        "d1", frozenset({SCOPE_TELEMETRY_WRITE}), 3600
    )
    case_log("凭据不存在-密钥错误", device_id="d1", given="bad", real=cred.secret[:8])
    with pytest.raises(TelemetryError) as ei:
        credentials.authenticate("d1", "bad-secret", SCOPE_TELEMETRY_WRITE)
    case_log("判定", reason=ei.value.reason.value)
    assert ei.value.reason is RejectReason.CREDENTIAL_NOT_FOUND


def test_credential_expired(credentials, cloud_store):
    cred = credentials.register_device(
        "d2", frozenset({SCOPE_TELEMETRY_WRITE}), 1
    )
    # 直接写入一个已过期的凭据，绕过等待真实时间。
    from edge_telemetry.models import DeviceCredential

    expired = DeviceCredential(
        device_id=cred.device_id,
        secret=cred.secret,
        scopes=cred.scopes,
        issued_at=utcnow() - timedelta(hours=2),
        expires_at=utcnow() - timedelta(seconds=1),
    )
    cloud_store.upsert_credential(expired)
    case_log(
        "凭据过期",
        device_id="d2",
        expires_at=expired.expires_at.isoformat(),
        now=utcnow().isoformat(),
    )
    with pytest.raises(TelemetryError) as ei:
        credentials.authenticate("d2", cred.secret, SCOPE_TELEMETRY_WRITE)
    case_log("判定", reason=ei.value.reason.value, detail=ei.value.detail)
    assert ei.value.reason is RejectReason.CREDENTIAL_EXPIRED


def test_scope_denied(credentials):
    cred = credentials.register_device(
        "d3", frozenset({SCOPE_REGISTER_DEVICE}), 3600
    )
    case_log(
        "越权上报",
        device_id="d3",
        scopes=sorted(cred.scopes),
        required=SCOPE_TELEMETRY_WRITE,
    )
    with pytest.raises(TelemetryError) as ei:
        credentials.authenticate("d3", cred.secret, SCOPE_TELEMETRY_WRITE)
    case_log("判定", reason=ei.value.reason.value, detail=ei.value.detail)
    assert ei.value.reason is RejectReason.SCOPE_DENIED


def test_three_reasons_are_distinct(credentials):
    reasons = {
        RejectReason.CREDENTIAL_NOT_FOUND.value,
        RejectReason.CREDENTIAL_EXPIRED.value,
        RejectReason.SCOPE_DENIED.value,
    }
    case_log("三类失败原因互斥", reasons=sorted(reasons))
    assert len(reasons) == 3
