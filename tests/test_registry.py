"""设备注册与凭据管理测试：失败原因必须可区分。"""

import logging

import pytest

from edge_telemetry.errors import ErrorCode, IngestionError
from edge_telemetry.models import CredentialScope
from edge_telemetry.registry import DeviceRegistry
from edge_telemetry.storage import MemoryStore

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test.registry")


def _clock_at(t: float):
    state = {"t": t}

    def now() -> float:
        return state["t"]

    now.set = lambda v: state.__setitem__("t", v)
    return now


def test_full_auth_flow_and_distinct_failures():
    clock = _clock_at(1000.0)
    reg = DeviceRegistry(MemoryStore(), clock=clock)
    dev = reg.register_device("dev-1")
    log.info("input=register dev-1 判定依据=注册成功 device=%s", dev.device_id)

    cred = reg.issue_credential(
        "dev-1", [CredentialScope.TELEMETRY_INGEST], ttl_seconds=100
    )
    log.info(
        "input=签发遥测凭据 ttl=100 判定依据=%s 过期时刻=%s",
        cred.credential_id, cred.expires_at,
    )

    # 正常通过
    ok = reg.authenticate(
        cred.credential_id, cred.secret, CredentialScope.TELEMETRY_INGEST
    )
    assert ok.device_id == "dev-1"
    log.info("input=正确凭据+正确scope 判定依据=鉴权通过")

    # 1) 凭据不存在
    with pytest.raises(IngestionError) as ei:
        reg.authenticate("no-such", "x", CredentialScope.TELEMETRY_INGEST)
    assert ei.value.code is ErrorCode.CREDENTIAL_NOT_FOUND
    log.info("input=未知凭据 判定依据=code=%s", ei.value.code.value)

    # 1b) 密钥错误同样归类为不存在（不暴露凭据是否存在）
    with pytest.raises(IngestionError) as ei:
        reg.authenticate(cred.credential_id, "wrong-secret",
                         CredentialScope.TELEMETRY_INGEST)
    assert ei.value.code is ErrorCode.CREDENTIAL_NOT_FOUND
    log.info("input=错误密钥 判定依据=code=%s", ei.value.code.value)

    # 2) 越权：凭据只有 ingest，要求 state:read
    with pytest.raises(IngestionError) as ei:
        reg.authenticate(cred.credential_id, cred.secret,
                         CredentialScope.STATE_READ)
    assert ei.value.code is ErrorCode.SCOPE_DENIED
    log.info("input=有效凭据申请 state:read 判定依据=code=%s granted=%s",
             ei.value.code.value, ei.value.details["granted"])

    # 3) 过期
    clock.set(cred.expires_at)
    with pytest.raises(IngestionError) as ei:
        reg.authenticate(cred.credential_id, cred.secret,
                         CredentialScope.TELEMETRY_INGEST)
    assert ei.value.code is ErrorCode.CREDENTIAL_EXPIRED
    log.info("input=时间推进到 expires_at 判定依据=code=%s now>=expires_at",
             ei.value.code.value)


def test_device_not_found_and_revocation():
    reg = DeviceRegistry(MemoryStore())
    with pytest.raises(IngestionError) as ei:
        reg.issue_credential("ghost", [CredentialScope.TELEMETRY_INGEST])
    assert ei.value.code is ErrorCode.DEVICE_NOT_FOUND
    log.info("input=给未注册设备签发凭据 判定依据=code=%s", ei.value.code.value)

    reg.register_device("d")
    cred = reg.issue_credential("d", [CredentialScope.TELEMETRY_INGEST])
    reg.revoke_credential(cred.credential_id)
    with pytest.raises(IngestionError) as ei:
        reg.authenticate(cred.credential_id, cred.secret,
                         CredentialScope.TELEMETRY_INGEST)
    assert ei.value.code is ErrorCode.CREDENTIAL_NOT_FOUND
    log.info("input=已撤销凭据 判定依据=code=%s", ei.value.code.value)


def test_register_is_idempotent():
    reg = DeviceRegistry(MemoryStore())
    a = reg.register_device("dup")
    b = reg.register_device("dup")
    assert a.registered_at == b.registered_at
    log.info("input=重复注册 dup 判定依据=返回同一设备，幂等")
