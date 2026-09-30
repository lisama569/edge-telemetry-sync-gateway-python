"""测试公共夹具与日志工具。

每个关键判定都会通过 ``case_log`` 打印：测试输入（设备、批次、序号、凭据状态）
与系统判定依据（原因码、水位、指纹、缓冲状态），便于追溯。
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from edge_telemetry.credentials import CredentialManager
from edge_telemetry.edge_agent import EdgeAgent
from edge_telemetry.ingest import CloudIngestService
from edge_telemetry.models import (
    SCOPE_TELEMETRY_WRITE,
    TelemetryBatch,
)
from edge_telemetry.storage import EdgeSpool, LocalStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("telemetry.test")


def case_log(title: str, **fields) -> None:
    parts = " ".join(f"{k}={v!r}" for k, v in fields.items())
    log.info("[%s] 输入/依据 -> %s", title, parts)


@pytest.fixture
def paths(tmp_path: Path):
    return tmp_path


@pytest.fixture
def cloud_store(paths):
    store = LocalStore(paths / "cloud.db")
    yield store
    store.close()


@pytest.fixture
def credentials(cloud_store):
    return CredentialManager(cloud_store)


@pytest.fixture
def cloud(cloud_store):
    return CloudIngestService(cloud_store)


@pytest.fixture
def make_spool(paths):
    created: list[EdgeSpool] = []

    def _make(capacity: int = 1000, name: str = "edge_spool.db") -> EdgeSpool:
        spool = EdgeSpool(paths / name, capacity=capacity)
        created.append(spool)
        return spool

    yield _make
    for spool in created:
        spool.close()


@pytest.fixture
def agent_factory(paths, cloud, credentials):
    """构造边缘代理；允许用同一个 spool 文件重建（模拟进程重启）。"""
    agents: list[EdgeAgent] = []

    def _make(capacity: int = 1000, online: bool = True,
              name: str = "edge_spool.db") -> EdgeAgent:
        spool = EdgeSpool(paths / name, capacity=capacity)
        agent = EdgeAgent(spool, cloud, credentials, online=online)
        agents.append(agent)
        return agent

    yield _make
    for agent in agents:
        agent._spool.close()


@pytest.fixture
def agent(agent_factory):
    return agent_factory()


def make_batch(device_id: str, seq: int, batch_id: str | None = None,
               payload=None) -> TelemetryBatch:
    return TelemetryBatch(
        device_id=device_id,
        batch_id=batch_id or f"b-{device_id}-{seq}",
        seq=seq,
        payload=payload if payload is not None else [{"v": seq}],
    )
