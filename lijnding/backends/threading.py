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
            while True:
                item = q_in.get()
                if item is SENTINEL:
                    q_in.task_done()
                    q_out.put(SENTINEL)
                    break

                try:
                    stage.metrics["items_in"] += 1
                    results = stage._invoke(context, item)
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
