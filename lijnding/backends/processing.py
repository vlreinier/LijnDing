from __future__ import annotations

import multiprocessing as mp
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from ..core.context import Context
from .base import BaseRunner

try:
    import dill as serializer
except ImportError:
    import pickle as serializer

if TYPE_CHECKING:
    from ..core.stage import Stage

SENTINEL = object()

def _worker_process(
    q_in: mp.Queue,
    q_out: mp.Queue,
    data_proxy,
    lock_proxy,
    stage_payload: bytes,
):
    """The function executed by each worker process."""
    from ..core.context import Context
    from ..core.utils import ensure_iterable

    worker_context = Context(_from_proxies=(data_proxy, lock_proxy))

    try:
        stage_func, _, inject_context = serializer.loads(stage_payload)
    except Exception as e:
        q_out.put(e)
        return

    while True:
        item = q_in.get()
        if item is SENTINEL:
            break
        try:
            if inject_context:
                results = stage_func(worker_context, item)
            else:
                results = stage_func(item)

            output_stream = ensure_iterable(results)
            for res in output_stream:
                q_out.put(res)
        except Exception as e:
            q_out.put(e)

    q_out.put(SENTINEL)

class ProcessingRunner(BaseRunner):
    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        # This backend is unstable and causes deadlocks in some environments.
        # It is left here as an experimental feature.
        raise NotImplementedError("The 'process' backend is currently unstable and disabled.")

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        raise NotImplementedError("The 'process' backend is currently unstable and disabled.")
