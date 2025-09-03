"""
An example demonstrating the use of the 'process' backend for CPU-bound tasks.
"""
import time
from lijnding.core import stage, Pipeline

# With a robust serializer like `dill`, the 'process' backend can even
# handle lambda functions, making it as easy to use as the 'thread' backend.
@stage(backend="process", workers=4)
def cpu_intensive_task(number: int):
    """
    A dummy function that simulates a CPU-bound task.
    """
    print(f"Processing {number}...")
    sum(range(number * 1_000_000))
    print(f"Finished {number}.")
    return number * number

def main():
    """Builds and runs the concurrent pipeline."""
    print("--- Starting pipeline with 'process' backend ---")

    pipeline = Pipeline([cpu_intensive_task])

    data = [8, 9, 7, 10]

    start_time = time.perf_counter()
    results, _ = pipeline.collect(data)
    end_time = time.perf_counter()

    print("\n--- Results ---")
    print(f"Results: {sorted(results)}")

    total_time = end_time - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds.")
    print("With 4 workers, this should be much faster than running sequentially.")

if __name__ == "__main__":
    # The `if __name__ == "__main__":` guard is crucial for
    # multiprocessing to work correctly on all platforms.
    main()
