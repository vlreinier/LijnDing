from __future__ import annotations

import multiprocessing as mp
from typing import TYPE_CHECKING, Any, Iterable, Iterator
import dill as serializer # Use dill for robust serialization

from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.stage import Stage
    from ..core.context import Context

SENTINEL = object()

def _worker_process(args):
    """
    A long-running worker process that waits for tasks on the input queue.
    """
    q_in, q_out, stage_payload, context_proxies = args
    from ..core.context import Context
    from ..core.utils import ensure_iterable

    stage_func, inject_context = serializer.loads(stage_payload)

    while True:
        item = q_in.get()
        if item is SENTINEL:
            break

        try:
            worker_context = None
            if context_proxies:
                worker_context = Context(_from_proxies=context_proxies)

            if inject_context:
                results = stage_func(worker_context, item)
            else:
                results = stage_func(item)

            for res in ensure_iterable(results):
                q_out.put(res)
        except Exception as e:
            q_out.put(e)

class ProcessingRunner(BaseRunner):
    """
    A runner that executes itemwise stages in a persistent pool of processes.
    This runner is performant for CPU-bound tasks.
    """
    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        try:
            # Spawn is the safest and most compatible start method
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            # It can only be set once per application
            pass

        workers = getattr(stage, "workers", 1)
        if workers <= 0:
            workers = mp.cpu_count()

        q_in: mp.Queue = mp.Queue(maxsize=workers * 2)
        q_out: mp.Queue = mp.Queue()

        context_proxies = (context._data, context._lock) if getattr(context, "_mp_safe", False) else None

        # Serialize the function once, to be sent to all workers
        stage_payload = serializer.dumps((stage.func, stage._inject_context))

        worker_args = (q_in, q_out, stage_payload, context_proxies)
        processes = [
            mp.Process(target=_worker_process, args=(worker_args,), daemon=True)
            for _ in range(workers)
        ]
        for p in processes:
            p.start()

        item_count = 0
        for item in iterable:
            item_count += 1
            q_in.put(item)

        # Send a sentinel for each worker to signal the end of work
        for _ in range(workers):
            q_in.put(SENTINEL)

        # Collect results
        for _ in range(item_count):
            result = q_out.get()
            if isinstance(result, Exception):
                raise result
            yield result

        # Clean up processes
        for p in processes:
            p.join(timeout=1.0)

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
