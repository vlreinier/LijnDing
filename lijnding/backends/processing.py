from __future__ import annotations

import multiprocessing as mp
from typing import TYPE_CHECKING, Any, Iterable, Iterator
import dill as serializer # Use dill for robust serialization

from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.stage import Stage
    from ..core.context import Context

SENTINEL = "__LIJNDING_WORKER_SENTINEL__"
ITEM_DONE = "__LIJNDING_ITEM_DONE__"

def _worker_process(
    q_in: mp.Queue,
    q_out: mp.Queue,
):
    """
    A long-running worker process that waits for tasks on the input queue.
    """
    from ..core.context import Context
    from ..core.utils import ensure_iterable

    while True:
        task = q_in.get()
        if task == SENTINEL:
            break

        try:
            stage_payload, (idx, item), context_proxies = task
            stage_func, inject_context = serializer.loads(stage_payload)

            worker_context = None
            if context_proxies:
                worker_context = Context(_from_proxies=context_proxies)

            if inject_context:
                results = stage_func(worker_context, item)
            else:
                results = stage_func(item)

            # A stage can produce multiple results for a single item.
            # We must associate all of them with the original index.
            for res in ensure_iterable(results):
                q_out.put((idx, res))
            # Signal that this item is done
            q_out.put((idx, ITEM_DONE))
        except Exception as e:
            # Propagate exceptions, associated with the index
            q_out.put((idx, e))

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

        # Serialize the function once, to be sent with each task
        stage_payload = serializer.dumps((stage.func, stage._inject_context))

        processes = [
            mp.Process(target=_worker_process, args=(q_in, q_out), daemon=True)
            for _ in range(workers)
        ]
        for p in processes:
            p.start()

        item_count = 0
        for i, item in enumerate(iterable):
            item_count += 1
            task = (stage_payload, (i, item), context_proxies)
            q_in.put(task)

        # All items have been sent. Now, tell workers to shut down when the
        # input queue is empty.
        for _ in range(workers):
            q_in.put(SENTINEL)

        # Collect results. This is more complex now because of out-of-order
        # execution and potential fan-out from stages.
        results_by_index = {}
        items_done_count = 0
        while items_done_count < item_count:
            # This will block until a result is available
            idx, res = q_out.get()

            if isinstance(res, Exception):
                # An exception in a worker should stop the pipeline
                raise res

            if res == ITEM_DONE:
                items_done_count += 1
                # If an item produced no results, its index might not be in the dict.
                # We add it here to preserve order for empty results.
                if idx not in results_by_index:
                    results_by_index[idx] = []
                continue

            if idx not in results_by_index:
                results_by_index[idx] = []
            results_by_index[idx].append(res)

        # Yield results in sorted order of their original index
        for i in sorted(results_by_index.keys()):
            for res in results_by_index[i]:
                yield res

        # Clean up processes
        for p in processes:
            p.join(timeout=1.0)

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
