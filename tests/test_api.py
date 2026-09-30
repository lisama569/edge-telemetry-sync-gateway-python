"""HTTP 端到端：注册、上报、鉴权原因码、边缘离线/回放。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from edge_telemetry.api import create_app
from tests.conftest import case_log


def _client(path):
    return TestClient(create_app(db_dir=str(path), capacity=2))


def test_full_lifecycle(tmp_path):
    c = _client(tmp_path)
    reg = c.post(
        "/devices/register",
        json={"device_id": "api1", "scopes": ["telemetry:write"], "ttl_seconds": 3600},
    ).json()
    secret = reg["secret"]
    headers = {"x-device-id": "api1", "x-device-secret": secret}

    ok = c.post(
        "/telemetry", headers=headers, json={"batch_id": "b1", "seq": 1, "payload": [{"v": 1}]}
    )
    case_log("HTTP 首次上报", status_code=ok.status_code, body=ok.json())
    assert ok.status_code == 200 and ok.json()["status"] == "accepted"

    dup = c.post(
        "/telemetry", headers=headers, json={"batch_id": "b1", "seq": 1, "payload": [{"v": 1}]}
    )
    assert dup.json()["status"] == "duplicate"

    bad_secret = c.post(
        "/telemetry",
        headers={"x-device-id": "api1", "x-device-secret": "wrong"},
        json={"batch_id": "b2", "seq": 2, "payload": []},
    )
    case_log("HTTP 密钥错误", body=bad_secret.json())
    assert bad_secret.json()["detail"]["reason"] == "credential_not_found"

    unknown = c.post(
        "/telemetry",
        headers={"x-device-id": "nobody", "x-device-secret": "x"},
        json={"batch_id": "b2", "seq": 1, "payload": []},
    )
    assert unknown.json()["detail"]["reason"] == "credential_not_found"


def test_scope_denied_over_http(tmp_path):
    c = _client(tmp_path)
    reg = c.post(
        "/devices/register",
        json={"device_id": "ro", "scopes": ["register:device"], "ttl_seconds": 3600},
    ).json()
    r = c.post(
        "/telemetry",
        headers={"x-device-id": "ro", "x-device-secret": reg["secret"]},
        json={"batch_id": "b", "seq": 1, "payload": []},
    )
    case_log("HTTP 越权", body=r.json())
    assert r.json()["detail"]["reason"] == "scope_denied"


def test_edge_offline_overflow_and_replay(tmp_path):
    c = _client(tmp_path)
    reg = c.post("/devices/register", json={"device_id": "edge1"}).json()
    headers = {"x-device-id": "edge1", "x-device-secret": reg["secret"]}

    c.post("/edge/link", json={"online": False})
    for seq in (1, 2):
        r = c.post(
            "/edge/submit",
            headers=headers,
            json={"batch_id": f"b{seq}", "seq": seq, "payload": [{"v": seq}]},
        )
        assert r.json()["status"] == "deferred"

    overflow = c.post(
        "/edge/submit",
        headers=headers,
        json={"batch_id": "b3", "seq": 3, "payload": []},
    )
    case_log("HTTP 缓冲溢出", status_code=overflow.status_code, body=overflow.json())
    assert overflow.status_code == 429
    assert overflow.json()["detail"]["reason"] == "buffer_full"

    c.post("/edge/link", json={"online": True})
    replay = c.post("/edge/replay").json()
    case_log("HTTP 回放", replayed=replay["replayed"], pending=replay["pending"])
    assert replay["replayed"] == 2 and replay["pending"] == 0

    state = c.get("/devices/edge1/state").json()
    case_log("HTTP 最终云端水位", next_expected=state["next_expected"])
    assert state["next_expected"] == 3
