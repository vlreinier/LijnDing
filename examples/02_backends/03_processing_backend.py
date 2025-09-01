"""
An example demonstrating the use of the 'process' backend for CPU-bound tasks.
"""
import time
from lijnding import stage, Pipeline

# NOTE: The 'process' backend is currently unstable and disabled in this version.
# This example shows how it *would* be used if it were functional.

@stage(backend="process", workers=4)
def cpu_intensive_task(number: int):
    """
    A dummy function that simulates a CPU-bound task.
    """
    sum(range(number * 1_000_000))
    return number * number

def main():
    """Builds and runs the concurrent pipeline."""
    print("--- Attempting to run pipeline with 'process' backend ---")

    pipeline = Pipeline([cpu_intensive_task])
    data = [8, 9, 7, 10]

    try:
        results, _ = pipeline.collect(data)
    except NotImplementedError as e:
        print(f"\nCaught expected error: {e}")
        print("The 'process' backend is disabled in this version of LijnDing.")

if __name__ == "__main__":
    main()
