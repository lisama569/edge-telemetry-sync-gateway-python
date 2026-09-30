"""边缘遥测接入与云端同步的本地 HTTP 演示服务。

仅使用本地存储（默认内存，可用环境变量 EDGE_TELEMETRY_STORE 指定 JSON 文件），
不依赖任何外部服务或真实凭据。启动后可访问 /docs，或直接运行：

    EDGE_TELEMETRY_DEMO=1 uv run python main.py

它会在进程内演示：注册 -> 离线缓冲 -> 弱网失败 -> 重连回放 -> 云边一致。
"""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from uvicorn import run

from edge_telemetry.edge import EdgeAgent
from edge_telemetry.errors import ErrorCode, IngestionError
from edge_telemetry.gateway import CloudGateway
from edge_telemetry.models import CredentialScope, OverflowPolicy, TelemetryBatch
from edge_telemetry.registry import DeviceRegistry
from edge_telemetry.storage import JsonFileStore, MemoryStore
from edge_telemetry.transport import LocalTransport

# ---- 单进程演示用的共享组件（本地存储，无线程外副作用）----

_store_path = os.environ.get("EDGE_TELEMETRY_STORE", "")
_store = JsonFileStore(_store_path) if _store_path else MemoryStore()
_registry = DeviceRegistry(_store)
_gateway = CloudGateway(_store, _registry)
_edges: dict[str, EdgeAgent] = {}


def _edge_for(device_id: str, credential_id: str, secret: str) -> EdgeAgent:
    """每个请求都先鉴权；通过后返回（或创建）该设备的边缘会话。

    凭据被轮换时，边缘会话改用本次通过鉴权的新凭据。
    """
    # 鉴权失败会抛 IngestionError（原因可区分），由路由转成 HTTP 错误
    cred = _registry.authenticate(
        credential_id, secret, CredentialScope.TELEMETRY_INGEST
    )
    if cred.device_id != device_id:
        raise IngestionError(
            ErrorCode.SCOPE_DENIED,
            "凭据不属于目标设备，禁止跨设备上报",
            details={"credential_device": cred.device_id,
                     "target_device": device_id},
        )
    edge = _edges.get(device_id)
    if edge is None:
        edge = EdgeAgent(
            device_id=device_id,
            store=_store,
            transport=LocalTransport(_gateway),
            registry=_registry,
            credential_id=credential_id,
            secret=secret,
            buffer_capacity=1000,
            overflow_policy=OverflowPolicy.REJECT_NEW,
        )
        _edges[device_id] = edge
    else:
        edge.credential_id = credential_id
        edge.secret = secret
    return edge


def _raise_http(exc: IngestionError) -> None:
    # 404 / 401 / 403 / 409 映射，body 中保留稳定 code 以便调用方区分原因
    status = {
        ErrorCode.DEVICE_NOT_FOUND: 404,
        ErrorCode.CREDENTIAL_NOT_FOUND: 401,
        ErrorCode.CREDENTIAL_EXPIRED: 401,
        ErrorCode.SCOPE_DENIED: 403,
        ErrorCode.SEQUENCE_CONFLICT: 409,
        ErrorCode.CONTENT_HASH_MISMATCH: 422,
        ErrorCode.BUFFER_FULL: 507,
        ErrorCode.REORDER_WINDOW_FULL: 509,
    }.get(exc.code, 400)
    raise HTTPException(
        status_code=status,
        detail={"code": exc.code.value, "message": exc.message,
                "details": exc.details},
    )


# ---- 请求模型 ----

class RegisterIn(BaseModel):
    device_id: str = Field(default_factory=lambda: "dev-" + uuid.uuid4().hex[:8])
    ttl_seconds: float = 3600.0


class PointIn(BaseModel):
    points: list[dict]
    device_id: str


class FlushIn(BaseModel):
    device_id: str
    online: bool = True


# ---- 应用 ----

@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.environ.get("EDGE_TELEMETRY_DEMO"):
        _run_scripted_demo()
    yield


app = FastAPI(
    title="边缘遥测接入与云端同步（本地模拟）",
    version="0.1.0",
    lifespan=lifespan,
)


@app.post("/devices/register")
def register_device(body: RegisterIn):
    device = _registry.register_device(body.device_id)
    cred = _registry.issue_credential(
        body.device_id, [CredentialScope.TELEMETRY_INGEST], body.ttl_seconds
    )
    return {
        "device_id": device.device_id,
        "credential_id": cred.credential_id,
        "secret": cred.secret,
        "scopes": cred.scopes,
        "expires_at": cred.expires_at,
    }


@app.post("/telemetry")
def report_telemetry(
    body: PointIn,
    x_credential_id: str = Header(...),
    x_credential_secret: str = Header(...),
):
    """边缘上报（经本地缓冲，离线也可接收，随后 /edge/flush 回放）。"""
    try:
        edge = _edge_for(body.device_id, x_credential_id, x_credential_secret)
        entry = edge.produce(body.points)
    except IngestionError as exc:
        _raise_http(exc)
    return {"batch_id": entry.batch_id, "seq": entry.seq, "buffered": True}


@app.post("/edge/flush")
def edge_flush(body: FlushIn):
    edge = _edges.get(body.device_id)
    if edge is None:
        raise HTTPException(404, detail={"code": "NO_EDGE",
                                        "message": "该设备尚无边缘会话"})
    edge.transport.set_online(body.online)  # type: ignore[attr-defined]
    return edge.flush()


@app.get("/edge/stats/{device_id}")
def edge_stats(device_id: str):
    edge = _edges.get(device_id)
    if edge is None:
        raise HTTPException(404, detail={"code": "NO_EDGE",
                                        "message": "该设备尚无边缘会话"})
    return edge.stats()


@app.get("/cloud/state/{device_id}")
def cloud_state(device_id: str):
    try:
        return _gateway.device_snapshot(device_id)
    except IngestionError as exc:
        _raise_http(exc)


# ---- 脚本式演示 ----

def _run_scripted_demo() -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("demo")

    device_id = "demo-device"
    dev = _registry.register_device(device_id)
    cred = _registry.issue_credential(
        device_id, [CredentialScope.TELEMETRY_INGEST], ttl_seconds=3600
    )
    transport = LocalTransport(_gateway)
    edge = EdgeAgent(device_id, _store, transport, _registry,
                     cred.credential_id, cred.secret)
    log.info("1) 注册设备 %s 并签发 telemetry:ingest 凭据", dev.device_id)

    transport.set_online(False)
    e1 = edge.produce([{"temp": 21.0}])
    e2 = edge.produce([{"temp": 22.5, "hum": 41}])
    log.info("2) 离线期间生产 seq=%s,%s（已持久化缓冲）", e1.seq, e2.seq)

    r = edge.flush()
    log.info("3) 离线回放: stopped=%s 剩余=%s（数据保留待重放）",
             r["stopped"], r["pending_remaining"])

    transport.set_online(True)
    transport.add_failures(1)
    r = edge.flush()
    log.info("4) 重连但首次遇瞬时弱网: stopped=%s 剩余=%s",
             r["stopped"], r["pending_remaining"])

    r = edge.flush()
    log.info("5) 再次回放: synced=%s 剩余=%s",
             r["synced"], r["pending_remaining"])

    # 重复重放同一批次必须幂等
    r = edge.flush()
    log.info("6) 空缓冲下重复回放: synced=%s（云端账本不产生重复）",
             r["synced"])

    check = edge.verify_consistency()
    log.info("7) 一致性: %s 云端状态=%s",
             check["consistent"], check["cloud"]["state"])


if __name__ == "__main__":
    demo = os.environ.get("EDGE_TELEMETRY_DEMO")
    if demo:
        _run_scripted_demo()
    else:
        run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8080")))
