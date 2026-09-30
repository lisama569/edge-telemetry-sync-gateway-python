"""云端接入：重复批次幂等、乱序延迟接受、序号冲突拒绝、并发安全。"""

from __future__ import annotations

import concurrent.futures

import pytest

from edge_telemetry.errors import RejectReason, TelemetryError
from tests.conftest import case_log, make_batch


def test_duplicate_batch_is_idempotent(cloud, cloud_store):
    b = make_batch("dev", 1)
    r1 = cloud.ingest(b)
    r2 = cloud.ingest(b)  # 完全重复上报
    case_log(
        "重复批次",
        batch_id=b.batch_id,
        first=r1.status,
        second=r2.status,
    )
    assert r1.status == "accepted"
    assert r2.status == "duplicate"
    assert r2.duplicate and r2.accepted
    rows = cloud_store.list_batches("dev")
    case_log("落库条数（应为1）", count=len(rows), seqs=[x["seq"] for x in rows])
    assert len(rows) == 1


def test_duplicate_same_batchid_different_payload_rejected(cloud):
    b1 = make_batch("dev", 1, payload=[{"v": 1}])
    b2 = make_batch("dev", 1, payload=[{"v": 999}])  # 同 batch_id 同 seq 不同内容
    cloud.ingest(b1)
    case_log(
        "同batch_id内容冲突",
        batch_id=b1.batch_id,
        fp1_payload=b1.payload,
        fp2_payload=b2.payload,
    )
    with pytest.raises(TelemetryError) as ei:
        cloud.ingest(b2)
    case_log("判定", reason=ei.value.reason.value, detail=ei.value.detail)
    assert ei.value.reason is RejectReason.SEQUENCE_CONFLICT


def test_same_seq_different_batch_conflict(cloud):
    b1 = make_batch("dev", 1, batch_id="A")
    b2 = make_batch("dev", 1, batch_id="B")
    cloud.ingest(b1)
    case_log("同序号不同批次", seq=1, batch_a="A", batch_b="B")
    with pytest.raises(TelemetryError) as ei:
        cloud.ingest(b2)
    case_log("判定", reason=ei.value.reason.value, detail=ei.value.detail)
    assert ei.value.reason is RejectReason.SEQUENCE_CONFLICT


def test_out_of_order_deferred_then_promoted(cloud, cloud_store):
    # 乱序：先到 seq=3，再到 seq=1（连带2缺失仍挂起），最后 seq=2 全部补齐。
    r3 = cloud.ingest(make_batch("dev", 3))
    case_log("先到 seq=3", status=r3.status, watermark=cloud_store.get_next_expected("dev"))
    assert r3.status == "deferred"

    r1 = cloud.ingest(make_batch("dev", 1))
    case_log(
        "再到 seq=1",
        status=r1.status,
        detail=r1.detail,
        watermark=cloud_store.get_next_expected("dev"),
    )
    assert r1.status == "accepted"
    assert cloud_store.get_next_expected("dev") == 2

    r2 = cloud.ingest(make_batch("dev", 2))
    case_log("最后到 seq=2", status=r2.status, detail=r2.detail)
    accepted = cloud_store.list_batches("dev", status="accepted")
    deferred = cloud_store.list_batches("dev", status="deferred")
    case_log(
        "最终状态",
        accepted_seqs=[x["seq"] for x in accepted],
        deferred_seqs=[x["seq"] for x in deferred],
        watermark=cloud_store.get_next_expected("dev"),
    )
    assert [x["seq"] for x in accepted] == [1, 2, 3]
    assert deferred == []
    assert cloud_store.get_next_expected("dev") == 4


def test_old_sequence_after_gap_filled_is_duplicate_or_conflict(cloud):
    cloud.ingest(make_batch("dev", 1))
    cloud.ingest(make_batch("dev", 2))
    # seq=1 再次用不同 batch_id/内容上报 -> 冲突，绝不覆盖。
    with pytest.raises(TelemetryError) as ei:
        cloud.ingest(make_batch("dev", 1, batch_id="other"))
    assert ei.value.reason is RejectReason.SEQUENCE_CONFLICT
    case_log("旧序号重放-冲突", reason=ei.value.reason.value)


def test_concurrent_same_batch_no_duplicate(cloud, cloud_store):
    b = make_batch("devc", 1)

    def submit():
        return cloud.ingest(b)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: submit(), range(16)))

    statuses = [r.status for r in results]
    accepted = [s for s in statuses if s == "accepted"]
    duplicates = [s for s in statuses if s == "duplicate"]
    case_log(
        "并发重复批次",
        total=len(statuses),
        accepted=len(accepted),
        duplicate=len(duplicates),
    )
    assert len(accepted) == 1
    assert len(duplicates) == 15
    assert len(cloud_store.list_batches("devc")) == 1


def test_concurrent_distinct_seqs_consistent_watermark(cloud, cloud_store):
    batches = [make_batch("devn", i) for i in range(1, 51)]

    def submit(batch):
        return cloud.ingest(batch)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(submit, batches))

    rows = cloud_store.list_batches("devn", status="accepted")
    case_log(
        "并发乱序补全",
        accepted_count=len(rows),
        seqs=[r["seq"] for r in rows][:5],
        watermark=cloud_store.get_next_expected("devn"),
    )
    assert [r["seq"] for r in rows] == list(range(1, 51))
    assert cloud_store.get_next_expected("devn") == 51
    # 内容未被覆盖：payload 中 v 与 seq 一致。
    import json

    for r in rows:
        assert json.loads(r["payload"]) == [{"v": r["seq"]}]
