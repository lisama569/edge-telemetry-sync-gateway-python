"""边缘侧：离线缓冲、重启不丢、缓冲溢出可观测、回放后云端/边缘一致。"""

from __future__ import annotations

import pytest

from edge_telemetry.errors import RejectReason, TelemetryError
from edge_telemetry.models import SCOPE_REGISTER_DEVICE, SCOPE_TELEMETRY_WRITE
from edge_telemetry.storage import EdgeSpool
from tests.conftest import case_log, make_batch


def test_offline_spool_then_replay_consistent(agent_factory, cloud_store, credentials):
    cred = credentials.register_device(
        "e1", frozenset({SCOPE_TELEMETRY_WRITE}), 3600
    )
    # 离线阶段：乱序 + 重复批次。
    offline = agent_factory(online=False, name="spool1.db")
    offline.submit(make_batch("e1", 1), cred.secret)
    offline.submit(make_batch("e1", 3), cred.secret)
    dup = offline.submit(make_batch("e1", 1), cred.secret)  # 离线重复
    case_log(
        "离线阶段",
        spooled=offline.metrics.spooled,
        dup=dup.status,
        pending=offline._spool.count_pending(),
    )
    assert dup.status == "duplicate"
    assert offline._spool.count_pending() == 2
    assert cloud_store.list_batches("e1") == []  # 云端尚不可见

    # 进程"重启"：用同一个 spool 文件重建 agent，缓冲仍在。
    restarted = agent_factory(online=True, name="spool1.db")
    survived = restarted._spool.count_pending()
    case_log("重启后缓冲恢复", pending=survived)
    assert survived == 2

    # 先补 seq=2（在线直传），再回放 1、3（回放时 1 补齐后 3 自动提升）。
    restarted.submit(make_batch("e1", 2), cred.secret)
    results = restarted.replay()
    case_log(
        "回放结果",
        results=[(r.batch_id, r.seq, r.status) for r in results],
        spool_remaining=restarted._spool.count_pending(),
    )
    accepted = cloud_store.list_batches("e1", status="accepted")
    case_log(
        "云端最终状态",
        accepted_seqs=[r["seq"] for r in accepted],
        watermark=cloud_store.get_next_expected("e1"),
    )
    assert restarted._spool.count_pending() == 0
    assert [r["seq"] for r in accepted] == [1, 2, 3]
    assert cloud_store.get_next_expected("e1") == 4


def test_replay_duplicate_already_in_cloud(agent_factory, cloud_store, credentials):
    cred = credentials.register_device(
        "e2", frozenset({SCOPE_TELEMETRY_WRITE}), 3600
    )
    offline = agent_factory(online=False, name="spool2.db")
    offline.submit(make_batch("e2", 1), cred.secret)
    # 另一条在线链路（或另一边缘节点）已把同批次送达云端。
    online_elsewhere = agent_factory(online=True, name="other2.db")
    online_elsewhere.submit(make_batch("e2", 1), cred.secret)

    restarted = agent_factory(online=True, name="spool2.db")
    results = restarted.replay()
    case_log(
        "回放云端已存在批次",
        status=results[0].status,
        detail=results[0].detail,
    )
    assert results[0].status == "duplicate"
    assert restarted._spool.count_pending() == 0
    assert len(cloud_store.list_batches("e2")) == 1


def test_buffer_overflow_reject_new_and_observable(agent_factory, credentials):
    cred = credentials.register_device(
        "e3", frozenset({SCOPE_TELEMETRY_WRITE}), 3600
    )
    agent = agent_factory(capacity=3, online=False, name="spool3.db")
    for i in range(1, 4):
        agent.submit(make_batch("e3", i), cred.secret)

    case_log("缓冲将满", capacity=3, pending=agent._spool.count_pending())
    with pytest.raises(TelemetryError) as ei:
        agent.submit(make_batch("e3", 4), cred.secret)
    case_log(
        "溢出判定",
        reason=ei.value.reason.value,
        detail=ei.value.detail,
        buffer_full_metric=agent.metrics.buffer_full,
    )
    assert ei.value.reason is RejectReason.BUFFER_FULL
    assert agent.metrics.buffer_full == 1
    assert agent._spool.count_pending() == 3  # 旧数据保留，未静默丢弃
    assert any("buffer-full" in e for e in agent.metrics.events)


def test_offline_auth_failures_are_distinguished(agent_factory, credentials):
    cred = credentials.register_device(
        "e4", frozenset({SCOPE_TELEMETRY_WRITE}), 3600
    )
    readonly = credentials.register_device(
        "e4b", frozenset({SCOPE_REGISTER_DEVICE}), 3600
    )
    agent = agent_factory(online=False, name="spool4.db")

    with pytest.raises(TelemetryError) as ei:
        agent.submit(make_batch("eX", 1), "whatever")
    assert ei.value.reason is RejectReason.CREDENTIAL_NOT_FOUND
    case_log("离线-凭据不存在", reason=ei.value.reason.value)

    with pytest.raises(TelemetryError) as ei:
        agent.submit(make_batch("e4b", 1), readonly.secret)
    assert ei.value.reason is RejectReason.SCOPE_DENIED
    case_log("离线-越权", reason=ei.value.reason.value)

    assert agent._spool.count_pending() == 0  # 鉴权失败不进缓冲


def test_spool_persistence_raw_sqlite(paths):
    """不经过 agent，直接验证 SQLite 文件在"重启"后可读。"""
    spool = EdgeSpool(paths / "raw.db", capacity=10)
    spool.add(make_batch("raw-dev", 1))
    spool.close()

    spool2 = EdgeSpool(paths / "raw.db", capacity=10)
    rows = spool2.pending_ordered()
    case_log("SQLite 文件重启恢复", rows=[(r["device_id"], r["seq"]) for r in rows])
    assert len(rows) == 1 and rows[0]["device_id"] == "raw-dev"
    spool2.close()
