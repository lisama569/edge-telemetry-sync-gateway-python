"""云端网关测试：幂等、乱序合并、序号冲突拒绝、并发约束。"""

import logging
import threading

import pytest

from edge_telemetry.errors import ErrorCode, IngestionError
from edge_telemetry.gateway import CloudGateway
from edge_telemetry.models import CredentialScope, TelemetryBatch
from edge_telemetry.registry import DeviceRegistry
from edge_telemetry.storage import MemoryStore

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test.gateway")


@pytest.fixture()
def setup():
    store = MemoryStore()
    reg = DeviceRegistry(store)
    gw = CloudGateway(store, reg, reorder_window=8)
    reg.register_device("d1")
    cred = reg.issue_credential(
        "d1", [CredentialScope.TELEMETRY_INGEST], ttl_seconds=3600
    )
    return store, reg, gw, cred


def _batch(seq, points):
    return TelemetryBatch(device_id="d1", seq=seq, points=points).normalized()


def test_in_order_merge_and_snapshot(setup):
    _, _, gw, cred = setup
    r1 = gw.ingest(_batch(1, [{"a": 1}]), cred.credential_id, cred.secret)
    r2 = gw.ingest(_batch(2, [{"b": 2}]), cred.credential_id, cred.secret)
    assert (r1.status, r2.status) == ("APPLIED", "APPLIED")
    snap = gw.device_snapshot("d1")
    assert snap["state"] == {"a": 1, "b": 2}
    assert snap["applied_seq"] == 2
    log.info("input=seq1,seq2 顺序上报 判定依据=水位线=2 状态=%s", snap["state"])


def test_duplicate_batch_is_idempotent(setup):
    store, _, gw, cred = setup
    b = _batch(1, [{"a": 1}])
    gw.ingest(b, cred.credential_id, cred.secret)
    r = gw.ingest(b, cred.credential_id, cred.secret)  # 同 batch_id+hash
    assert r.duplicate is True and r.status == "APPLIED"
    snap = gw.device_snapshot("d1")
    assert snap["state"] == {"a": 1}
    ledger = list(snap["ledger"].values())
    assert len(ledger) == 1  # 重复批次不产生重复数据
    log.info("input=相同批次重复两次 判定依据=duplicate=True 账本仅 1 条")


def test_out_of_order_then_gap_fill(setup):
    _, _, gw, cred = setup
    cid, sec = cred.credential_id, cred.secret

    r3 = gw.ingest(_batch(3, [{"c": 3}]), cid, sec)
    r2 = gw.ingest(_batch(2, [{"b": 2}]), cid, sec)
    assert r3.status == "DEFERRED" and r2.status == "DEFERRED"
    log.info("input=seq3,seq2 早到 判定依据=均 DEFERRED，水位线仍=0")
    assert gw.device_snapshot("d1")["state"] == {}

    r1 = gw.ingest(_batch(1, [{"a": 1}]), cid, sec)
    assert r1.status == "APPLIED" and r1.applied_seq == 3
    snap = gw.device_snapshot("d1")
    assert snap["state"] == {"a": 1, "b": 2, "c": 3}
    assert snap["pending_seqs"] == []
    log.info("input=seq1 补齐缺口 判定依据=级联提升到3 状态=%s", snap["state"])


def test_same_seq_conflicting_content_rejected(setup):
    _, _, gw, cred = setup
    cid, sec = cred.credential_id, cred.secret
    gw.ingest(_batch(1, [{"a": 1}]), cid, sec)
    # 同序号不同内容（不同 batch_id 与 hash）
    clash = TelemetryBatch(device_id="d1", seq=1,
                           points=[{"a": 999}]).normalized()
    with pytest.raises(IngestionError) as ei:
        gw.ingest(clash, cid, sec)
    assert ei.value.code is ErrorCode.SEQUENCE_CONFLICT
    snap = gw.device_snapshot("d1")
    assert snap["state"] == {"a": 1}  # 未被静默覆盖
    log.info("input=seq1 内容冲突重报 判定依据=SEQUENCE_CONFLICT，状态保持原值")


def test_pending_conflict_rejected(setup):
    _, _, gw, cred = setup
    cid, sec = cred.credential_id, cred.secret
    gw.ingest(_batch(2, [{"b": 2}]), cid, sec)  # 进缓冲
    clash = TelemetryBatch(device_id="d1", seq=2,
                           points=[{"b": 200}]).normalized()
    with pytest.raises(IngestionError) as ei:
        gw.ingest(clash, cid, sec)
    assert ei.value.code is ErrorCode.SEQUENCE_CONFLICT
    log.info("input=缓冲中 seq2 内容冲突 判定依据=SEQUENCE_CONFLICT 不覆盖")


def test_cross_device_credential_is_scope_denied(setup):
    _, reg, gw, cred = setup
    reg.register_device("d2")
    foreign = TelemetryBatch(device_id="d2", seq=1,
                             points=[{"z": 0}]).normalized()
    with pytest.raises(IngestionError) as ei:
        gw.ingest(foreign, cred.credential_id, cred.secret)
    assert ei.value.code is ErrorCode.SCOPE_DENIED
    log.info("input=d1凭据给d2上报 判定依据=SCOPE_DENIED 跨设备越权")


def test_reorder_window_full(setup):
    _, _, gw, cred = setup
    cid, sec = cred.credential_id, cred.secret
    # 窗口=8：先缓存 seq2..9（8 个），第 9 个应被拒绝
    for s in range(2, 10):
        gw.ingest(_batch(s, [{"k": s}]), cid, sec)
    with pytest.raises(IngestionError) as ei:
        gw.ingest(_batch(10, [{"k": 10}]), cid, sec)
    assert ei.value.code is ErrorCode.REORDER_WINDOW_FULL
    log.info("input=乱序缓冲达到8 后再来 seq10 判定依据=REORDER_WINDOW_FULL")


def test_concurrent_same_batch_and_distinct_seqs(setup):
    _, _, gw, cred = setup
    cid, sec = cred.credential_id, cred.secret
    errors: list[Exception] = []

    # 同一个批次被 8 个线程并发上报：只能并入一次
    b = _batch(1, [{"a": 1}])
    barrier = threading.Barrier(8)

    def send_same():
        barrier.wait()
        try:
            gw.ingest(b, cid, sec)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=send_same) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert gw.device_snapshot("d1")["state"] == {"a": 1}
    assert len(gw.device_snapshot("d1")["ledger"]) == 1
    log.info("input=8线程并发同一批次 判定依据=无异常且仅并入一次")

    # 乱序并发不同序号（seq 2..50）。窗口只有 8，超出的批次会收到
    # REORDER_WINDOW_FULL（由边缘侧重试，属预期），这里校验确定不变量：
    # 无冲突异常、状态只含已并入的连续前缀、无重复数据、缓冲不超限。
    window_errors: list[str] = []
    seqs = list(range(2, 51))
    seqs.reverse()

    def send_seq(s):
        try:
            gw.ingest(_batch(s, [{"n": s}]), cid, sec)
        except IngestionError as e:
            if e.code is ErrorCode.REORDER_WINDOW_FULL:
                window_errors.append(e.code.value)
            else:
                errors.append(e)

    ts = [threading.Thread(target=send_seq, args=(s,)) for s in seqs]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert errors == []
    snap = gw.device_snapshot("d1")
    wm = snap["applied_seq"]
    # 状态必须是连续前缀合并结果（每批写同一个键 n，最终值=水位线），
    # 无静默覆盖其它键、无重复批次。
    assert snap["state"] == {"a": 1, "n": wm} if wm >= 2 else snap["state"] == {"a": 1}
    assert len(snap["pending_seqs"]) <= 8
    # 账本中 APPLIED 数量 == 水位线（无重复批次）
    applied = [v for v in snap["ledger"].values() if v["status"] == "APPLIED"]
    assert len(applied) == wm
    log.info(
        "input=49线程乱序并发 seq2..50(窗口8) 判定依据=水位线=%s 缓冲=%s "
        "溢出拒绝=%s，连续前缀无重复",
        wm, len(snap["pending_seqs"]), len(window_errors),
    )
