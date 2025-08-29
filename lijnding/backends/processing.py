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
        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        workers = getattr(stage, "workers", 1)
        if workers <= 0:
            workers = mp.cpu_count()

        q_in: mp.Queue = mp.Queue(maxsize=workers * 2)
        q_out: mp.Queue = mp.Queue()

        if not getattr(context, "_mp_safe", False):
            raise TypeError("A process-safe Context is required for the ProcessingRunner.")

        stage_payload = serializer.dumps((stage.func, stage.name, stage._inject_context))

        processes = [
            mp.Process(
                target=_worker_process,
                args=(q_in, q_out, context._data, context._lock, stage_payload),
                daemon=True
            )
            for _ in range(workers)
        ]
        for p in processes:
            p.start()

        # Main process feeds the queue directly, removing the feeder process
        data_list = list(iterable)
        for item in data_list:
            q_in.put(item)
        for _ in range(workers):
            q_in.put(SENTINEL)

        finished_workers = 0
        while finished_workers < workers:
            result = q_out.get()
            if result is SENTINEL:
                finished_workers += 1
                continue

            if isinstance(result, Exception):
                raise result

            yield result

        for p in processes:
            p.join(timeout=1.0)

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
