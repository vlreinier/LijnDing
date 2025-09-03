"""
An example demonstrating the use of the Context object and Hooks to share
state and collect metrics across a pipeline.
"""
from lijnding.core import stage, aggregator_stage, Context, Hooks

# --- Hook Definition ---

def print_metrics_on_end(context: Context):
    """
    This hook function is triggered when the stage it's attached to has
    received all items from the upstream pipeline. It's a perfect place
    to perform final calculations or print summary metrics.
    """
    total_items = context.get("items_processed")
    error_items = context.get("error_items")

    print("\n--- Metrics from on_stream_end Hook ---")
    print(f"Total items processed: {total_items}")
    print(f"Items with errors: {error_items}")

# --- Stage Definitions ---

@stage
def process_data(context: Context, data: dict):
    """
    A stage that processes data and uses the context to track metrics.
    The context is a thread-safe dictionary that is shared across all stages
    in a pipeline run.
    """
    # Increment a counter in the context for each item processed.
    context.inc("items_processed")

    # Handle a simulated error condition.
    if data.get("is_error"):
        context.inc("error_items")
        # Returning None (or not yielding) effectively filters out this item.
        return None

    # Set a value in the context only if it's not already there.
    if context.get("start_time") is None:
        import time
        context.set("start_time", time.time())

    return data["value"]

# The `on_stream_end` hook is attached to the aggregator stage. This means
# `print_metrics_on_end` will be called after `process_data` has finished
# processing all items, but before `sum_values` receives the final iterable.
@aggregator_stage(hooks=Hooks(on_stream_end=print_metrics_on_end))
def sum_values(iterable):
    """
    An aggregator stage that sums the values from the input stream.
    """
    yield sum(iterable)


def main():
    data = [
        {"value": 10},
        {"value": 20},
        {"is_error": True},
        {"value": 30},
    ]

    pipeline = process_data | sum_values

    results, final_context = pipeline.collect(data)

    print("\n--- Final Result ---")
    print(f"Sum of values: {results[0]}")

    print("\n--- Final Context Object ---")
    print(final_context.to_dict())


if __name__ == "__main__":
    main()
