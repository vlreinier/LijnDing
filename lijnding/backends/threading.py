from __future__ import annotations

import queue
import threading
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from ..core.utils import ensure_iterable
from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


SENTINEL = object()


class ThreadingRunner(BaseRunner):
    """
    A runner that executes itemwise stages in a pool of threads.
    Aggregator stages are run serially.
    """

    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        workers = getattr(stage, "workers", 1)
        if workers <= 0:
            workers = 1

        maxsize = workers * 2
        q_in: queue.Queue[Any] = queue.Queue(maxsize=maxsize)
        q_out: queue.Queue[Any] = queue.Queue(maxsize=maxsize)

        def feeder():
            for item in iterable:
                q_in.put(item)
            for _ in range(workers):
                q_in.put(SENTINEL)

        def worker():
            import copy
            worker_context = copy.copy(context)
            worker_context.worker_state = {}

            try:
                # Call the worker initialization hook if it exists
                if stage.hooks.on_worker_init:
                    try:
                        worker_context.worker_state = stage.hooks.on_worker_init(worker_context) or {}
                    except Exception as e:
                        stage.logger.error(f"Error in on_worker_init: {e}", exc_info=True)
                        q_out.put(e)  # Propagate error to main thread
                        return # Terminate worker

                while True:
                    item = q_in.get()
                    if item is SENTINEL:
                        break

                    try:
                        stage.metrics["items_in"] += 1
                        results = stage._invoke(worker_context, item)
                        output_stream = ensure_iterable(results)

                        count = 0
                        for res in output_stream:
                            stage.metrics["items_out"] += 1
                            count += 1
                            q_out.put(res)

                        stage.logger.debug(f"Successfully processed item, produced {count} output item(s).")
                    except Exception as e:
                        stage.logger.warning(f"Error processing item: {e}", exc_info=True)
                        q_out.put(e)
                    finally:
                        q_in.task_done()

            finally:
                # Signal that this worker is done processing items
                q_in.task_done()
                q_out.put(SENTINEL)

                # Call the worker exit hook if it exists
                if stage.hooks.on_worker_exit:
                    try:
                        stage.hooks.on_worker_exit(worker_context)
                    except Exception as e:
                        stage.logger.error(f"Error in on_worker_exit: {e}", exc_info=True)
                        # We can't easily propagate this error, so we just log it.

        feeder_thread = threading.Thread(target=feeder, daemon=True)
        feeder_thread.start()

        threads = [threading.Thread(target=worker, daemon=True) for i in range(workers)]
        for t in threads:
            t.start()

        finished_workers = 0
        while finished_workers < workers:
            try:
                result = q_out.get(timeout=0.1)
                if result is SENTINEL:
                    finished_workers += 1
                    continue

                if isinstance(result, Exception):
                    raise result

                yield result

            except queue.Empty:
                if not any(t.is_alive() for t in threads) and q_out.empty():
                    break

        feeder_thread.join(timeout=1.0)
        for t in threads:
            t.join(timeout=1.0)

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
