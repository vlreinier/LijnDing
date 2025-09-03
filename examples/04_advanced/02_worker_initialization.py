"""
An example demonstrating how to use the `on_worker_init` hook to initialize
a resource once per worker in concurrent backends.
"""
import time
import os
import threading
from lijnding.core import Pipeline, stage, Hooks
from lijnding.core.context import Context

def init_worker(context: Context) -> dict:
    """
    This function is called once per worker.
    It simulates creating an expensive, stateful resource, like a database
    connection or a large machine learning model.
    """
    # Use the context logger, which is properly configured.
    context.logger.info(f"Initializing worker... (PID: {os.getpid()}, Thread: {threading.get_ident()})")
    time.sleep(1)  # Simulate expensive initialization

    # The returned dictionary is stored in `context.worker_state`
    return {
        "worker_id": f"worker_{os.getpid()}_{threading.get_ident()}",
        "initialized_at": time.time()
    }

# Create a Hooks object and assign our function to the on_worker_init hook.
# This is how you attach the initialization logic to a stage.
worker_hooks = Hooks(on_worker_init=init_worker)

@stage(backend="thread", workers=4, hooks=worker_hooks)
def process_item(context: Context, item: int):
    """
    A stage that uses the initialized resource from the context.
    """
    # Access the worker-specific state that was created by the hook.
    worker_id = context.worker_state.get("worker_id", "unknown")

    context.logger.info(f"Processing item {item} on {worker_id}")
    time.sleep(0.2)
    return item * 2

def main():
    """Builds and runs the pipeline."""
    print("--- Starting pipeline with worker initialization ---")

    # Create a simple pipeline with our stage
    pipeline = Pipeline() | process_item

    # Run the pipeline
    start_time = time.perf_counter()
    results, _ = pipeline.collect(range(12))
    end_time = time.perf_counter()

    print("\n--- Results ---")
    print(f"Results: {results}")

    total_time = end_time - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds.")
    print("Note that the initialization (4 workers * 1s sleep) happens concurrently.")
    print("The total time should be a little over (1s init + 12 items * 0.2s / 4 workers).")

if __name__ == "__main__":
    main()
