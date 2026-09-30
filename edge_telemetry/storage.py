"""本地持久化存储（SQLite 文件，模拟边缘侧与云端存储）。

进程重启后数据不丢；同进程内通过锁串行化写事务，
支撑"同一设备并发上报"场景下的幂等与序号约束。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from edge_telemetry.models import DeviceCredential, TelemetryBatch, utcnow


def _iso(dt: object) -> str:
    return dt.isoformat() if not isinstance(dt, str) else dt


def _parse_dt(value: str):
    from datetime import datetime

    return datetime.fromisoformat(value)


class LocalStore:
    """云端侧本地存储：设备凭据 + 已接收/延迟批次 + 设备序号水位。"""

    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL");
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS credentials (
                    device_id  TEXT PRIMARY KEY,
                    secret     TEXT NOT NULL,
                    scopes     TEXT NOT NULL,
                    issued_at  TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    device_id    TEXT NOT NULL,
                    seq          INTEGER NOT NULL,
                    batch_id     TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    payload      TEXT NOT NULL,
                    status       TEXT NOT NULL,          -- accepted | deferred
                    received_at  TEXT NOT NULL,
                    PRIMARY KEY (device_id, seq)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_batches_batch ON batches(device_id, batch_id);
                CREATE TABLE IF NOT EXISTS device_state (
                    device_id       TEXT PRIMARY KEY,
                    next_expected   INTEGER NOT NULL      -- 下一个可立即接受的序号
                );
                """
            )

    # ---- 凭据 ----

    def upsert_credential(self, cred: DeviceCredential) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO credentials(device_id, secret, scopes, issued_at, expires_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(device_id) DO UPDATE SET
                     secret=excluded.secret, scopes=excluded.scopes,
                     issued_at=excluded.issued_at, expires_at=excluded.expires_at""",
                (
                    cred.device_id,
                    cred.secret,
                    json.dumps(sorted(cred.scopes), ensure_ascii=False),
                    _iso(cred.issued_at),
                    _iso(cred.expires_at),
                ),
            )

    def get_credential(self, device_id: str) -> DeviceCredential | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM credentials WHERE device_id=?", (device_id,)
            ).fetchone()
        if row is None:
            return None
        return DeviceCredential(
            device_id=row["device_id"],
            secret=row["secret"],
            scopes=frozenset(json.loads(row["scopes"])),
            issued_at=_parse_dt(row["issued_at"]),
            expires_at=_parse_dt(row["expires_at"]),
        )

    # ---- 设备序号水位 ----

    def get_next_expected(self, device_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT next_expected FROM device_state WHERE device_id=?",
                (device_id,),
            ).fetchone()
        return int(row["next_expected"]) if row else 1

    def _set_next_expected(self, device_id: str, value: int) -> None:
        self._conn.execute(
            """INSERT INTO device_state(device_id, next_expected) VALUES(?, ?)
               ON CONFLICT(device_id) DO UPDATE SET next_expected=excluded.next_expected""",
            (device_id, value),
        )

    # ---- 批次 ----

    def get_batch_by_batch_id(
        self, device_id: str, batch_id: str
    ) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM batches WHERE device_id=? AND batch_id=?",
                (device_id, batch_id),
            ).fetchone()

    def get_batch_by_seq(self, device_id: str, seq: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM batches WHERE device_id=? AND seq=?",
                (device_id, seq),
            ).fetchone()

    def insert_batch(
        self,
        batch: TelemetryBatch,
        content_hash: str,
        status: str,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO batches(device_id, seq, batch_id, content_hash,
                                      payload, status, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    batch.device_id,
                    batch.seq,
                    batch.batch_id,
                    content_hash,
                    json.dumps(batch.payload, ensure_ascii=False, sort_keys=True),
                    status,
                    _iso(utcnow()),
                ),
            )

    def promote_due_batches(self, device_id: str) -> list[int]:
        """沿连续序号把 deferred 批次提升为 accepted，推进水位。

        判定依据是"该序号槽位存在批次"（无论刚 accepted 的还是早先 deferred 的）；
        遇到第一个缺失序号即停止。返回本次新提升（由 deferred 转 accepted）的序号。
        """
        promoted: list[int] = []
        with self._lock, self._conn:
            nxt = self.get_next_expected(device_id)
            while True:
                row = self._conn.execute(
                    "SELECT status FROM batches WHERE device_id=? AND seq=?",
                    (device_id, nxt),
                ).fetchone()
                if row is None:
                    break
                if row["status"] == "deferred":
                    self._conn.execute(
                        "UPDATE batches SET status='accepted' WHERE device_id=? AND seq=?",
                        (device_id, nxt),
                    )
                    promoted.append(nxt)
                nxt += 1
            self._set_next_expected(device_id, nxt)
        return promoted

    def list_batches(self, device_id: str, status: str | None = None):
        sql = "SELECT * FROM batches WHERE device_id=?"
        params: list[object] = [device_id]
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY seq"
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class EdgeSpool:
    """边缘侧本地持久缓冲：待回放批次队列（FIFO，容量受限）。"""

    def __init__(self, db_path: str | Path, capacity: int) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.capacity = capacity
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL");
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS spool (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id   TEXT NOT NULL,
                    batch_id    TEXT NOT NULL,
                    seq         INTEGER NOT NULL,
                    payload     TEXT NOT NULL,
                    produced_at TEXT NOT NULL,
                    enqueued_at TEXT NOT NULL,
                    UNIQUE(device_id, batch_id)
                );
                """
            )

    def get(self, device_id: str, batch_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM spool WHERE device_id=? AND batch_id=?",
                (device_id, batch_id),
            ).fetchone()

    def count_pending(self) -> int:
        with self._lock:
            return int(
                self._conn.execute("SELECT COUNT(*) AS c FROM spool").fetchone()["c"]
            )

    def oldest(self) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM spool ORDER BY id ASC LIMIT 1"
            ).fetchone()

    def add(self, batch: TelemetryBatch) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO spool(device_id, batch_id, seq, payload,
                                     produced_at, enqueued_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    batch.device_id,
                    batch.batch_id,
                    batch.seq,
                    json.dumps(batch.payload, ensure_ascii=False, sort_keys=True),
                    _iso(batch.produced_at),
                    _iso(utcnow()),
                ),
            )

    def remove(self, spool_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM spool WHERE id=?", (spool_id,))

    def pending_ordered(self) -> list[sqlite3.Row]:
        """确定性回放顺序：先设备、再序号、再入队时间/id。"""
        with self._lock:
            return [
                r
                for r in self._conn.execute(
                    "SELECT * FROM spool ORDER BY device_id ASC, seq ASC, id ASC"
                ).fetchall()
            ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
