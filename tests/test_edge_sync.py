"""边缘侧测试：离线缓冲、重启恢复、弱网回放、溢出策略、最终一致性。"""

import logging

import pytest

from edge_telemetry.edge import EdgeAgent
from edge_telemetry.errors import ErrorCode, IngestionError
from edge_telemetry.gateway import CloudGateway
from edge_telemetry.models import CredentialScope, OverflowPolicy
from edge_telemetry.registry import DeviceRegistry
from edge_telemetry.storage import JsonFileStore
from edge_telemetry.transport import LocalTransport

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test.edge")


def _bootstrap(path, device_id="d1", ttl=3600.0, clock=None):
    store = JsonFileStore(path)
    reg = DeviceRegistry(store, clock=clock) if clock else DeviceRegistry(store)
    reg.register_device(device_id)
    cred = reg.issue_credential(
        device_id, [CredentialScope.TELEMETRY_INGEST], ttl_seconds=ttl
    )
    gw = CloudGateway(store, reg, clock=clock) if clock else CloudGateway(store, reg)
    transport = LocalTransport(gw)
    edge = EdgeAgent(device_id, store, transport, reg,
                     cred.credential_id, cred.secret)
    return store, reg, gw, transport, edge, cred


def _restart(path, cred, device_id="d1", **edge_kw):
    """模拟进程重启：从同一文件重建全部组件，凭据沿用。"""
    store = JsonFileStore(path)
    reg = DeviceRegistry(store)
    gw = CloudGateway(store, reg)
    transport = LocalTransport(gw)
    edge = EdgeAgent(device_id, store, transport, reg,
                     cred.credential_id, cred.secret, **edge_kw)
    return store, reg, gw, transport, edge


def test_offline_buffer_then_replay_consistent(tmp_path):
    path = str(tmp_path / "kv.json")
    store, reg, gw, transport, edge, cred = _bootstrap(path)

    transport.set_online(False)
    e1 = edge.produce([{"temp": 1}])
    e2 = edge.produce([{"temp": 2}])
    e3 = edge.produce([{"temp": 3, "hum": 40}])
    log.info("input=离线下生产 seq=%s 判定依据=全部入本地 outbox",
             [e1.seq, e2.seq, e3.seq])

    r = edge.flush()
    assert r["stopped"] is True and r["pending_remaining"] == 3
    assert edge.stats()["pending"] == 3
    log.info("input=离线 flush 判定依据=transport_failures=1，数据仍保留")

    transport.set_online(True)
    r = edge.flush()
    assert r["pending_remaining"] == 0 and r["synced"] == [
        e1.batch_id, e2.batch_id, e3.batch_id
    ]
    log.info("input=恢复在线 flush 判定依据=按 seq 顺序全部同步")

    check = edge.verify_consistency()
    assert check["consistent"] is True
    assert check["cloud"]["state"] == {"temp": 3, "hum": 40}
    log.info("input=一致性校验 判定依据=云边水位线与状态一致 %s",
             check["cloud"])


def test_restart_does_not_lose_unsynced_data(tmp_path):
    path = str(tmp_path / "kv.json")
    store, reg, gw, transport, edge, cred = _bootstrap(path)

    transport.set_online(False)
    a = edge.produce([{"x": 1}])
    b = edge.produce([{"y": 2}])
    del store, reg, gw, transport, edge  # 模拟进程退出（文件已落盘）
    log.info("input=离线生产2批后退出进程 判定依据=outbox 已原子落盘")

    # 进程重启，云端与边缘都从同一文件恢复；先离线再上线回放
    store2, reg2, gw2, transport2, edge2 = _restart(path, cred)
    assert edge2.stats()["pending"] == 2
    log.info("input=重启后读取 outbox 判定依据=pending=2，序号计数器也持久化")
    c = edge2.produce([{"z": 3}])  # 序号必须继续递增，不复用
    assert c.seq == 3

    transport2.set_online(True)
    edge2.flush()
    check = edge2.verify_consistency()
    assert check["consistent"] is True
    assert check["cloud"]["applied_seq"] == 3
    assert check["cloud"]["state"] == {"x": 1, "y": 2, "z": 3}
    log.info("input=重启后回放+新数据 判定依据=水位线=3 状态连续无丢失")


def test_replay_is_idempotent_after_weak_network(tmp_path):
    path = str(tmp_path / "kv.json")
    _, _, _, transport, edge, _ = _bootstrap(path)

    e = edge.produce([{"t": 1}])
    # 云端实际可能已处理，但边缘未收到响应：模拟一次瞬时失败
    transport.add_failures(1)
    r1 = edge.flush()
    assert r1["stopped"] is True and r1["pending_remaining"] == 1
    # 但 LocalTransport 的失败发生在 ingest 之前，云端没有该批；
    # 第二次发送成功。再人为重放一次已 SYNCED 批次也必须幂等。
    edge.flush()
    assert edge.stats()["synced"] == 1

    from edge_telemetry.models import TelemetryBatch
    dup = TelemetryBatch(device_id="d1", seq=e.seq, points=e.points,
                         batch_id=e.batch_id, hash=e.hash)
    result = transport.send(dup, edge.credential_id, edge.secret)
    assert result.duplicate is True
    log.info("input=弱网重试+重复批次 判定依据=duplicate=True，云端无重复数据")
    snap = transport.gateway.device_snapshot("d1")
    assert len(snap["ledger"]) == 1


def test_buffer_full_reject_new_is_observable(tmp_path):
    path = str(tmp_path / "kv.json")
    _, _, _, transport, edge, _ = _restart_with_capacity(
        path, capacity=3, policy=OverflowPolicy.REJECT_NEW
    )
    transport.set_online(False)
    for i in range(3):
        edge.produce([{"k": i}])
    with pytest.raises(IngestionError) as ei:
        edge.produce([{"k": 3}])
    assert ei.value.code is ErrorCode.BUFFER_FULL
    stats = edge.stats()
    assert stats["pending"] == 3
    overflow_events = [ev for ev in stats["events"]
                       if ev["kind"] == "buffer_overflow"]
    assert overflow_events and overflow_events[0]["action"] == "rejected_new"
    log.info("input=缓冲达上限后再生产 判定依据=BUFFER_FULL，事件可观测且未丢旧数据")


def test_buffer_overflow_drop_oldest_is_observable(tmp_path):
    path = str(tmp_path / "kv.json")
    _, _, _, transport, edge, cred = _restart_with_capacity(
        path, capacity=3, policy=OverflowPolicy.DROP_OLDEST
    )
    transport.set_online(False)
    first = edge.produce([{"k": 0}])
    edge.produce([{"k": 1}])
    edge.produce([{"k": 2}])
    fourth = edge.produce([{"k": 3}])  # 触发淘汰最旧
    stats = edge.stats()
    pending_ids = [x.batch_id for x in edge._load_outbox("PENDING")]
    assert first.batch_id not in pending_ids
    assert fourth.batch_id in pending_ids
    assert stats["dropped"] == 1
    ev = [x for x in stats["events"] if x["kind"] == "buffer_overflow"][0]
    assert ev["action"] == "dropped_oldest"
    assert ev["evicted_batch_id"] == first.batch_id
    log.info("input=DROP_OLDEST 下第4批 判定依据=淘汰 %s，事件含被淘汰批次",
             first.batch_id)


def test_expired_credential_during_flush_marks_dead(tmp_path):
    path = str(tmp_path / "kv.json")
    clock = {"t": 1000.0}
    store, reg, gw, transport, edge, cred = _bootstrap(
        path, ttl=100, clock=lambda: clock["t"]
    )
    edge.produce([{"t": 1}])
    clock["t"] = 2000.0  # 凭据过期后回放
    r = edge.flush()
    assert r["dead"] == 1 and r["pending_remaining"] == 0
    stats = edge.stats()
    assert stats["dead_entries"] == 1
    ev = [x for x in stats["events"] if x["kind"] == "terminal_sync_failure"][0]
    assert ev["code"] == ErrorCode.CREDENTIAL_EXPIRED.value
    log.info("input=凭据过期后回放 判定依据=终态 DEAD + 事件 code=CREDENTIAL_EXPIRED")


def _restart_with_capacity(path, capacity, policy):
    """注册设备+凭据后以指定缓冲策略构造一套组件。"""
    store, reg, gw, transport, edge, cred = _bootstrap(path)
    _, _, _, transport, edge = _restart(
        path, cred, buffer_capacity=capacity, overflow_policy=policy
    )
    return store, reg, gw, transport, edge, cred


def test_concurrent_produce_same_device_keeps_unique_seqs(tmp_path):
    path = str(tmp_path / "kv.json")
    _, _, _, transport, edge, _cred = _bootstrap(path, )
    transport.set_online(False)

    n = 20
    entries: list = []
    errors: list[Exception] = []
    import threading
    barrier = threading.Barrier(n)

    def produce(i):
        barrier.wait()
        try:
            e = edge.produce([{"i": i}])
            entries.append(e)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    ts = [threading.Thread(target=produce, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    assert errors == []
    seqs = sorted(e.seq for e in entries)
    assert seqs == list(range(1, n + 1))  # 并发下序号唯一且连续
    log.info("input=%s 线程并发 produce 判定依据=序号集合恰为 1..%s", n, n)

    transport.set_online(True)
    edge.flush()
    check = edge.verify_consistency()
    assert check["consistent"] is True
    assert check["cloud"]["applied_seq"] == n
    log.info("input=并发批次回放 判定依据=云边一致 水位线=%s",
             check["cloud"]["applied_seq"])
