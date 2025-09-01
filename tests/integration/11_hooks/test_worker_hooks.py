import multiprocessing as mp
import os
import threading
import time

import pytest

from lijnding import Pipeline, stage, Hooks
from lijnding.core.context import Context

# A manager and queue that can be used by multiple processes in the tests
manager = mp.Manager()
event_queue = manager.Queue()

def clear_queue():
    while not event_queue.empty():
        event_queue.get_nowait()

# --- Test Implementations ---

def init_func(context: Context):
    # Use a combination of PID and Thread ID for a unique worker identifier
    worker_id = f"{os.getpid()}_{threading.get_ident()}"
    # Store the unique ID in the worker-local state.
    context.worker_state["id"] = worker_id
    event_queue.put(f"init_{worker_id}")

def exit_func(context: Context):
    # Retrieve the unique ID from the worker-local state.
    worker_id = context.worker_state.get("id", "unknown")
    event_queue.put(f"exit_{worker_id}")

class UnserializableState:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.lock = threading.Lock() # Locks are not serializable

    def init(self, context: Context):
        event_queue.put(f"init_{self.worker_id}")

    def exit(self, context: Context):
        event_queue.put(f"exit_{self.worker_id}")

@pytest.fixture(autouse=True)
def before_each():
    """Clears the queue before each test."""
    clear_queue()
    yield
    clear_queue()

@pytest.mark.parametrize("backend,workers", [("thread", 4), ("process", 2)])
def test_init_and_exit_hooks_are_called_once_per_worker(backend, workers):
    """
    Tests that on_worker_init and on_worker_exit are called exactly
    once for each worker.
    """
    hooks = Hooks(on_worker_init=init_func, on_worker_exit=exit_func)

    @stage(backend=backend, workers=workers, hooks=hooks)
    def simple_stage(context: Context, item: int):
            # This stage function is just a passthrough. The main logic for the
            # test is in the init and exit hooks.
        return item

    pipeline = Pipeline() | simple_stage
    pipeline.collect(range(workers * 2)) # Run some data through

    time.sleep(0.5) # Allow time for exit hooks to fire

    # Collect all events
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
    workers = 2
    state_manager = UnserializableState("test_worker")
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
    workers = 2
    state_manager = UnserializableState("test_worker")
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
