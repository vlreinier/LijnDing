from __future__ import annotations

import multiprocessing as mp
import threading
from typing import Any, Dict, Optional


class Context:
    """
    A dict-like context for sharing state and metrics across pipeline stages.
    """

    def __init__(self, mp_safe: bool = False, initial_data: Optional[Dict[str, Any]] = None, *, _from_proxies=None):
        if _from_proxies:
            # Reconstruct from existing manager proxies
            self._data, self._lock = _from_proxies
            self._mp_safe = True
            return

        self._mp_safe = mp_safe
        self._manager = None
        if mp_safe:
            self._manager = mp.Manager()
            self._data = self._manager.dict()
            self._lock = self._manager.Lock()
        else:
            self._data: Dict[str, Any] = {}
            self._lock = threading.Lock()

        if initial_data:
            self.update(initial_data)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def update(self, other: Dict[str, Any]) -> None:
        with self._lock:
            for k, v in other.items():
                self._data[k] = v

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def inc(self, key: str, amount: int = 1) -> int:
        with self._lock:
            current_value = self._data.get(key, 0)
            new_value = int(current_value) + amount
            self._data[key] = new_value
            return new_value

    def __repr__(self) -> str:
        return f"Context(mp_safe={self._mp_safe}, data={self.to_dict()})"
