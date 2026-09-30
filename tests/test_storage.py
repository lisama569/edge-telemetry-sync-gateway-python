"""存储层测试：原子落盘与重启恢复。"""

import logging

from edge_telemetry.storage import JsonFileStore, MemoryStore

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test.storage")


def test_memory_store_crud():
    s = MemoryStore()
    s.put("a", {"x": 1})
    assert s.get("a") == {"x": 1}
    assert s.get("missing", "def") == "def"
    s.put("ab", 2)
    assert s.prefix("a") == {"a": {"x": 1}, "ab": 2}
    s.delete("a")
    assert s.keys() == ["ab"]
    log.info("input=put/get/prefix/delete 判定依据=内存 KV 行为符合预期")


def test_json_file_store_survives_restart(tmp_path):
    path = str(tmp_path / "kv.json")
    s1 = JsonFileStore(path)
    s1.put("device:d1", {"applied_seq": 3, "state": {"temp": 1}})
    s1.put("outbox:d1:b1", {"seq": 1, "status": "PENDING"})
    log.info("input=写入两条记录后释放存储 判定依据=模拟进程退出")

    s2 = JsonFileStore(path)  # 重新构造 = 进程重启
    assert s2.get("device:d1")["applied_seq"] == 3
    assert s2.get("outbox:d1:b1")["status"] == "PENDING"
    s2.delete("outbox:d1:b1")
    log.info("input=重启后读取 判定依据=未同步条目仍存在，删除后持久化")

    s3 = JsonFileStore(path)
    assert s3.get("outbox:d1:b1") is None
