from __future__ import annotations

import multiprocessing as mp
import warnings
from typing import TYPE_CHECKING, Any, Iterable, Iterator
import dill as serializer

from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.stage import Stage
    from ..core.context import Context

def _worker_process(q_in: mp.Queue, q_out: mp.Queue):
    """
    A simple worker that processes one item and exits.
    It expects a tuple of (stage_payload, item) on the queue.
    """
    from ..core.utils import ensure_iterable

    stage_payload, item, context_proxies = q_in.get()

    # Recreate context if proxies are provided
    worker_context = None
    if context_proxies:
        from ..core.context import Context
        worker_context = Context(_from_proxies=context_proxies)

    try:
        stage_func, inject_context = serializer.loads(stage_payload)

        if inject_context:
            results = stage_func(worker_context, item)
        else:
            results = stage_func(item)

        output_stream = ensure_iterable(results)
        for res in output_stream:
            q_out.put(res)

    except Exception as e:
        q_out.put(e)

class ProcessingRunner(BaseRunner):
    """
    A runner that executes itemwise stages in separate processes.

    This runner is designed for stability and compatibility, especially in
    test environments. It achieves this by spawning a new process for each
    individual item in the input stream.

    .. warning::
        This implementation is NOT performant for large datasets due to the
        overhead of process creation for every item. It is intended for use
        in scenarios where process isolation is critical and performance is
        not the primary concern.
    """
    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        warnings.warn(
            "The 'process' backend is not performant for large datasets as it "
            "spawns a new process for each item.",
            UserWarning
        )
        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        q_in: mp.Queue = mp.Queue()
        q_out: mp.Queue = mp.Queue()

        stage_payload = serializer.dumps((stage.func, stage._inject_context))

        # Prepare context proxies if needed
        context_proxies = (context._data, context._lock) if getattr(context, "_mp_safe", False) else None

        processes = []
        for item in iterable:
            p = mp.Process(target=_worker_process, args=(q_in, q_out))
            p.start()
            processes.append(p)
            q_in.put((stage_payload, item, context_proxies))

        for p in processes:
            result = q_out.get()
            if isinstance(result, Exception):
                raise result
            yield result
            p.join()

    def _run_aggregator(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        from .serial import SerialRunner
        return SerialRunner()._run_aggregator(stage, context, iterable)
