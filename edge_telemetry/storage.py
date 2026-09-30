"""持久化存储抽象与 JSON 文件实现（进程重启不丢数据）。"""

from __future__ import annotations

import abc
import json
import os
import tempfile
import threading
from typing import Any


class KVStore(abc.ABC):
    """字符串键值存储抽象。值为可 JSON 序列化对象。"""

    @abc.abstractmethod
    def get(self, key: str, default: Any = None) -> Any: ...

    @abc.abstractmethod
    def put(self, key: str, value: Any) -> None: ...

    @abc.abstractmethod
    def delete(self, key: str) -> None: ...

    @abc.abstractmethod
    def keys(self) -> list[str]: ...

    @abc.abstractmethod
    def prefix(self, prefix: str) -> dict[str, Any]: ...

    @abc.abstractmethod
    def flush(self) -> None: ...

    @abc.abstractmethod
    def close(self) -> None: ...


class _BaseStore(KVStore):
    """带锁的内存字典基类。"""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def keys(self) -> list[str]:
        with self._lock:
            return sorted(self._data.keys())

    def prefix(self, prefix: str) -> dict[str, Any]:
        with self._lock:
            return {k: v for k, v in self._data.items() if k.startswith(prefix)}

    def flush(self) -> None:  # 内存实现无需落盘
        return None

    def close(self) -> None:
        return None


class MemoryStore(_BaseStore):
    """进程内 KV，用于测试与瞬态场景。"""


class JsonFileStore(_BaseStore):
    """单文件 JSON KV。

    - 构造时若文件存在则加载（进程重启恢复）；
    - 每次 ``put/delete`` 后原子落盘（临时文件 + os.replace），保证崩溃不丢；
    - 所有读写在同一把可重入锁内串行化，配合上层每设备锁可安全并发。
    """

    def __init__(self, path: str):
        super().__init__()
        self.path = path
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict):
                raise ValueError(f"存储文件格式非法: {self.path}")
            self._data = loaded

    def _write_locked(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".kv-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self._write_locked()

    def delete(self, key: str) -> None:
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._write_locked()

    def flush(self) -> None:
        with self._lock:
            self._write_locked()
