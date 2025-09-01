from __future__ import annotations

import multiprocessing as mp
from typing import TYPE_CHECKING, Any, Iterable, Iterator
import dill as serializer # Use dill for robust serialization

from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.stage import Stage
    from ..core.context import Context

SENTINEL = "__LIJNDING_SENTINEL__"

def _worker_process(
    q_in: mp.Queue,
    q_out: mp.Queue,
):
    """
    A long-running worker process that waits for tasks on the input queue.
    """
    from ..core.context import Context
    from ..core.utils import ensure_iterable

    stage = None
    worker_context = None

    try:
        while True:
            task = q_in.get()
            if task == SENTINEL:
                break

            try:
                stage_payload, item, context_proxies = task

                # One-time initialization for the worker process
                if stage is None:
                    stage = serializer.loads(stage_payload)
                    worker_context = Context(_from_proxies=context_proxies)

                    # Call the worker initialization hook if it exists
                    if stage.hooks.on_worker_init:
                        try:
                            worker_context.worker_state = stage.hooks.on_worker_init(worker_context) or {}
                        except Exception as e:
                            # Propagate the error and terminate the worker
                            q_out.put(e)
                            break

                # Invoke the stage logic. This will also handle the context.logger injection.
                results = stage._invoke(worker_context, item)

                for res in ensure_iterable(results):
                    q_out.put(res)

            except Exception as e:
                # Errors are caught and sent back to the main process to be handled
                # by the pipeline's error policy.
                q_out.put(e)
    finally:
        # Signal that this worker is done
        q_out.put(SENTINEL)

        # Call the worker exit hook if it exists
        if stage and worker_context and stage.hooks.on_worker_exit:
            try:
                stage.hooks.on_worker_exit(worker_context)
            except Exception:
                # Cannot easily propagate this error, so we can't do much.
                # A sophisticated logger could send this to a central service.
                pass

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

        # Serialize the entire stage object once.
        # This is safe because we are using 'dill' which can handle complex objects.
        stage_payload = serializer.dumps(stage)

        processes = [
            mp.Process(target=_worker_process, args=(q_in, q_out), daemon=False)
            for _ in range(workers)
        ]
        for p in processes:
            p.start()

        # Feeder logic: put all items and then one sentinel per worker onto the queue
        for item in iterable:
            task = (stage_payload, item, context_proxies)
            q_in.put(task)

        for _ in range(workers):
            q_in.put(SENTINEL)

        # Collector logic: read from the output queue until we've received a sentinel
        # from each worker.
        finished_workers = 0
        while finished_workers < workers:
            result = q_out.get()

            if result == SENTINEL:
                finished_workers += 1
                continue

            if isinstance(result, Exception):
                # The worker encountered an error, re-raise it in the main process
                raise result

            yield result

        # Clean up processes
        for p in processes:
            p.join(timeout=1.0)

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
