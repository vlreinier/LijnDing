from __future__ import annotations

import multiprocessing as mp
import threading
import time
from typing import TYPE_CHECKING, Any, Iterable, Iterator
import dill as serializer

from .base import BaseRunner, _handle_error_routing

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage

SENTINEL = "__LIJNDING_SENTINEL__"
METRICS_SENTINEL = "__METRICS__"
ERROR_SENTINEL = "__ERROR__"

def _worker_process(
    q_in: mp.Queue,
    q_out: mp.Queue,
    stage_payload: bytes,
    context_proxies: tuple | None,
    worker_id: int,
):
    """Worker process that initializes a stage and processes items."""
    from ..core.context import Context
    from ..core.utils import ensure_iterable
    from ..core.log import get_logger

    logger = get_logger(f"lijnding.worker.{worker_id}")
    try:
        stage = serializer.loads(stage_payload)
        worker_context = Context(_from_proxies=context_proxies)

        if stage.hooks.on_worker_init:
            worker_context.worker_state = stage.hooks.on_worker_init(worker_context) or {}

        logger.info("worker_started")

        while True:
            item = q_in.get()
            if item == SENTINEL:
                break

            item_start_time = time.perf_counter()
            try:
                results = stage._invoke(worker_context, item)
                count_out = 0
                for res in ensure_iterable(results):
                    q_out.put(res)
                    count_out += 1

                item_elapsed = time.perf_counter() - item_start_time
                metrics = {"items_in": 1, "items_out": count_out, "errors": 0, "time_total": item_elapsed}
                q_out.put((METRICS_SENTINEL, metrics))

            except Exception as e:
                item_elapsed = time.perf_counter() - item_start_time
                metrics = {"items_in": 1, "items_out": 0, "errors": 1, "time_total": item_elapsed}
                q_out.put((METRICS_SENTINEL, metrics))
                q_out.put((ERROR_SENTINEL, (item, e)))

    except Exception as e:
        logger.error("worker_critical_error", error=str(e))
        q_out.put((ERROR_SENTINEL, (None, e))) # Signal critical failure
    finally:
        if stage and worker_context and stage.hooks.on_worker_exit:
            stage.hooks.on_worker_exit(worker_context)
        logger.info("worker_finished")
        q_out.put(SENTINEL)


class ProcessingRunner(BaseRunner):
    """A runner that executes itemwise stages in a persistent pool of processes."""

    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        stage.logger.info("stream_started", backend="processing", workers=stage.workers)
        stream_start_time = time.perf_counter()

        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        q_in: mp.Queue = mp.Queue(maxsize=stage.buffer_size or (stage.workers * 2))
        q_out: mp.Queue = mp.Queue()

        context_proxies = (context._data, context._lock) if getattr(context, "_mp_safe", False) else None
        stage_payload = serializer.dumps(stage)

        processes = [
            mp.Process(
                target=_worker_process,
                args=(q_in, q_out, stage_payload, context_proxies, i),
                daemon=True
            ) for i in range(stage.workers)
        ]
        for p in processes:
            p.start()

        total_items_fed = 0
        def feeder():
            nonlocal total_items_fed
            try:
                for item in iterable:
                    q_in.put(item)
                    total_items_fed += 1
            finally:
                for _ in range(stage.workers):
                    q_in.put(SENTINEL)

        feeder_thread = threading.Thread(target=feeder, daemon=True)
        feeder_thread.start()

        finished_workers = 0
        try:
            while finished_workers < stage.workers:
                result = q_out.get()

                if result == SENTINEL:
                    finished_workers += 1
                    continue

                if isinstance(result, tuple):
                    if result[0] == METRICS_SENTINEL:
                        for key, value in result[1].items():
                            stage.metrics[key] += value
                        continue

                    if result[0] == ERROR_SENTINEL:
                        item, e = result[1]
                        stage.logger.warning(f"Error from worker: {e}")
                        if item is None: # Critical worker error
                            raise e

                        policy = stage.error_policy
                        if policy.mode == "route_to_stage":
                            _handle_error_routing(stage, context, item)
                        elif policy.mode != "skip":
                            raise e
                        continue

                yield result
        finally:
            for p in processes:
                p.join(timeout=1.0)
                if p.is_alive():
                    p.terminate()

            total_duration = time.perf_counter() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=stage.metrics['items_in'],
                items_out=stage.metrics['items_out'],
                errors=stage.metrics['errors'],
                duration=round(total_duration, 4),
            )

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
