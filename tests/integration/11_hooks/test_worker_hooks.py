import multiprocessing as mp
import os
import threading
import time

import pytest

from lijnding import Pipeline, stage, Hooks
from lijnding.core.context import Context

# --- Test Implementations ---

def make_hooks(queue: mp.Queue) -> Hooks:
    """Factory to create hooks that close over a queue for testing."""
    def init_func(context: Context):
        worker_id = f"{os.getpid()}_{threading.get_ident()}"
        queue.put(f"init_{worker_id}")
        return {"id": worker_id}

    def exit_func(context: Context):
        worker_id = context.worker_state.get("id", "unknown")
        queue.put(f"exit_{worker_id}")

    return Hooks(on_worker_init=init_func, on_worker_exit=exit_func)


class UnserializableState:
    def __init__(self, worker_id: str, queue: mp.Queue):
        self.worker_id = worker_id
        self.queue = queue
        self.lock = threading.Lock()  # Locks are not serializable

    def init(self, context: Context):
        self.queue.put(f"init_{self.worker_id}")
        return {}

    def exit(self, context: Context):
        self.queue.put(f"exit_{self.worker_id}")


@pytest.mark.parametrize("backend,workers", [("thread", 4), ("process", 2)])
def test_init_and_exit_hooks_are_called_once_per_worker(backend, workers):
    """
    Tests that on_worker_init and on_worker_exit are called exactly
    once for each worker.
    """
    with mp.Manager() as manager:
        event_queue = manager.Queue()
        hooks = make_hooks(event_queue)

        @stage(backend=backend, workers=workers, hooks=hooks)
        def simple_stage(context: Context, item: int):
            return item

        pipeline = Pipeline() | simple_stage
        pipeline.collect(range(workers * 2))

        time.sleep(0.5)

        init_events = set()
        exit_events = set()
        while not event_queue.empty():
            event = event_queue.get_nowait()
            if event.startswith("init"):
                init_events.add(event)
            elif event.startswith("exit"):
                exit_events.add(event)

        assert len(init_events) == workers
        assert len(exit_events) == workers


def test_threading_hook_with_unserializable_state():
    """
    Tests that the 'thread' backend can use hooks with captured
    unserializable state (like locks or other complex objects).
    """
    with mp.Manager() as manager:
        event_queue = manager.Queue()
        workers = 2
        state_manager = UnserializableState("test_worker", event_queue)
        hooks = Hooks(on_worker_init=state_manager.init, on_worker_exit=state_manager.exit)

        @stage(backend="thread", workers=workers, hooks=hooks)
        def a_stage(item: int):
            return item

        pipeline = Pipeline() | a_stage
        pipeline.collect(range(workers))

        time.sleep(0.5)

        init_events = 0
        exit_events = 0
        while not event_queue.empty():
            event = event_queue.get_nowait()
            if event.startswith("init"):
                init_events += 1
            elif event.startswith("exit"):
                exit_events += 1

        assert init_events == workers
        assert exit_events == workers

def test_processing_hook_with_unserializable_state_fails():
    """
    Tests that the 'process' backend fails to serialize a stage
    if the hook captures unserializable state. This confirms the limitation.
    """
    with mp.Manager() as manager:
        # We need a queue to instantiate UnserializableState, even though the test
        # should fail before the queue is ever used.
        dummy_queue = manager.Queue()
        workers = 2
        state_manager = UnserializableState("test_worker", dummy_queue)
        hooks = Hooks(on_worker_init=state_manager.init) # Just init is enough to fail

    @stage(backend="process", workers=workers, hooks=hooks)
    def a_stage(item: int):
        return item

    pipeline = Pipeline() | a_stage

    # This should fail because the 'state_manager' instance with its lock
    # cannot be serialized by dill to be sent to the worker processes.
    with pytest.raises(Exception) as excinfo:
        pipeline.collect(range(workers))

    # We expect a serialization error (TypeError from dill/pickle)
    assert isinstance(excinfo.value, TypeError)
