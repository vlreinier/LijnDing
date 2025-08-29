from __future__ import annotations

import multiprocessing as mp
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from ..core.context import Context
from .base import BaseRunner
from .function_registry import get_function, register_function

if TYPE_CHECKING:
    from ..core.stage import Stage

SENTINEL = object()

def _feeder_process(q_in: mp.Queue, iterable: Iterable[Any], workers: int):
    """A target function for the feeder process, must be top-level."""
    for item in iterable:
        q_in.put(item)
    for _ in range(workers):
        q_in.put(SENTINEL)

def _worker_process(
    q_in: mp.Queue,
    q_out: mp.Queue,
    data_proxy,
    lock_proxy,
    stage_func_name: str,
    inject_context: bool,
):
    """The function executed by each worker process."""
    from ..core.context import Context
    from ..core.utils import ensure_iterable

    worker_context = Context(_from_proxies=(data_proxy, lock_proxy))
    stage_func = get_function(stage_func_name)

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

        # Pre-flight check for registered function
        func_name = f"{stage.func.__module__}.{stage.func.__name__}"
        try:
            get_function(stage.name or func_name)
        except NameError as e:
            raise TypeError(
                f"Stage '{stage.name}' is not registered for multiprocessing. "
                f"Please add `register_for_processing=True` to the @stage decorator."
            ) from e

        workers = getattr(stage, "workers", 1)
        if workers <= 0:
            workers = mp.cpu_count()

        q_in: mp.Queue = mp.Queue(maxsize=workers * 2)
        q_out: mp.Queue = mp.Queue()

        if not getattr(context, "_mp_safe", False):
            raise TypeError("A process-safe Context is required for the ProcessingRunner.")

        data_list = list(iterable)

        feeder_proc = mp.Process(target=_feeder_process, args=(q_in, data_list, workers), daemon=True)
        feeder_proc.start()

        processes = [
            mp.Process(
                target=_worker_process,
                args=(q_in, q_out, context._data, context._lock, stage.name or func_name, stage._inject_context),
                daemon=True
            )
            for _ in range(workers)
        ]
        for p in processes:
            p.start()

        finished_workers = 0
        while finished_workers < workers:
            result = q_out.get()
            if result is SENTINEL:
                finished_workers += 1
                continue

            if isinstance(result, Exception):
                raise result

            yield result

        feeder_proc.join(timeout=1.0)
        for p in processes:
            p.join(timeout=1.0)

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        # Aggregators are not typically run in parallel processes as they need the full iterable.
        # Running them in a single separate process could be an option, but for now, we delegate to serial.
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
