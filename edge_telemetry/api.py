"""FastAPI 接线层。

端点：
- POST /devices/register              注册设备，返回凭据（含有效期与作用范围）
- POST /telemetry                     设备直连云端上报（凭据头鉴权）
- POST /edge/submit                   经边缘代理上报（在线直传/离线缓冲）
- POST /edge/link                     切换边缘链路 online/offline
- POST /edge/replay                   重连回放
- GET  /devices/{id}/state            查看云端已接收批次与序号水位
- GET  /edge/metrics                  边缘缓冲与可观测指标
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from edge_telemetry.credentials import CredentialManager
from edge_telemetry.edge_agent import EdgeAgent
from edge_telemetry.ingest import CloudIngestService
from edge_telemetry.models import (
    SCOPE_TELEMETRY_WRITE,
    TelemetryBatch,
    utcnow,
)
from edge_telemetry.storage import EdgeSpool, LocalStore

#: 边缘缓冲默认容量（条数）。
DEFAULT_BUFFER_CAPACITY = 1000


class RegisterIn(BaseModel):
    device_id: str
    scopes: list[str] = Field(default_factory=lambda: [SCOPE_TELEMETRY_WRITE])
    ttl_seconds: int = 3600


class TelemetryIn(BaseModel):
    batch_id: str
    seq: int
    payload: list[dict]
    produced_at: datetime | None = None


class LinkIn(BaseModel):
    online: bool


def create_app(db_dir: str = ".local_data", capacity: int = DEFAULT_BUFFER_CAPACITY) -> FastAPI:
    base = Path(db_dir)
    cloud_store = LocalStore(base / "cloud.db")
    spool = EdgeSpool(base / "edge_spool.db", capacity=capacity)
    credentials = CredentialManager(cloud_store)
    cloud = CloudIngestService(cloud_store)
    agent = EdgeAgent(spool, cloud, credentials, online=True)

    app = FastAPI(title="edge-telemetry-sync-gateway")
    app.state.cloud_store = cloud_store
    app.state.spool = spool
    app.state.credentials = credentials
    app.state.cloud = cloud
    app.state.agent = agent

    def _to_batch(device_id: str, body: TelemetryIn) -> TelemetryBatch:
        return TelemetryBatch(
            device_id=device_id,
            batch_id=body.batch_id,
            seq=body.seq,
            payload=body.payload,
            produced_at=body.produced_at or utcnow(),
        )

    @app.post("/devices/register")
    def register_device(body: RegisterIn):
        cred = credentials.register_device(
            body.device_id, frozenset(body.scopes), body.ttl_seconds
        )
        return {
            "device_id": cred.device_id,
            "secret": cred.secret,
            "scopes": sorted(cred.scopes),
            "issued_at": cred.issued_at.isoformat(),
            "expires_at": cred.expires_at.isoformat(),
        }

    @app.post("/telemetry")
    def telemetry_direct(
        body: TelemetryIn,
        x_device_id: str = Header(...),
        x_device_secret: str = Header(...),
    ):
        from edge_telemetry.errors import TelemetryError

        try:
            credentials.authenticate(
                x_device_id, x_device_secret, SCOPE_TELEMETRY_WRITE
            )
            result = cloud.ingest(_to_batch(x_device_id, body))
        except TelemetryError as exc:
            raise HTTPException(
                status_code=403,
                detail={"reason": exc.reason.value, "message": exc.detail},
            )
        return {
            "device_id": result.device_id,
            "batch_id": result.batch_id,
            "seq": result.seq,
            "status": result.status,
            "detail": result.detail,
        }

    @app.post("/edge/submit")
    def edge_submit(
        body: TelemetryIn,
        x_device_id: str = Header(...),
        x_device_secret: str = Header(...),
    ):
        from edge_telemetry.errors import TelemetryError

        try:
            result = agent.submit(_to_batch(x_device_id, body), x_device_secret)
        except TelemetryError as exc:
            code = 429 if exc.reason.value == "buffer_full" else 403
            raise HTTPException(
                status_code=code,
                detail={"reason": exc.reason.value, "message": exc.detail},
            )
        return {
            "device_id": result.device_id,
            "batch_id": result.batch_id,
            "seq": result.seq,
            "status": result.status,
            "detail": result.detail,
        }

    @app.post("/edge/link")
    def edge_link(body: LinkIn):
        agent.set_online(body.online)
        return {"online": agent.online}

    @app.post("/edge/replay")
    def edge_replay():
        results = agent.replay()
        return {
            "replayed": len(results),
            "pending": spool.count_pending(),
            "results": [
                {
                    "device_id": r.device_id,
                    "batch_id": r.batch_id,
                    "seq": r.seq,
                    "status": r.status,
                    "detail": r.detail,
                }
                for r in results
            ],
        }

    @app.get("/devices/{device_id}/state")
    def device_state(device_id: str):
        return {
            "device_id": device_id,
            "next_expected": cloud_store.get_next_expected(device_id),
            "batches": cloud_store.list_batches(device_id),
        }

    @app.get("/edge/metrics")
    def edge_metrics():
        m = agent.metrics
        return {
            "online": agent.online,
            "capacity": spool.capacity,
            "pending": spool.count_pending(),
            "submitted": m.submitted,
            "forwarded": m.forwarded,
            "spooled": m.spooled,
            "duplicate_offline": m.duplicate_offline,
            "buffer_full": m.buffer_full,
            "replayed": m.replayed,
            "replay_failures": m.replay_failures,
            "events": m.events,
        }

    return app
