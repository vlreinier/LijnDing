"""
An example demonstrating the use of the Context object and Hooks to share
state and collect metrics across a pipeline.
"""
from lijnding import stage, aggregator_stage, Context, Hooks

# --- Hook Definition ---

def print_metrics_on_end(context: Context):
    """
    A hook function that will be called after the stream feeding into the
    aggregator has been completely processed.
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
    """
    context.inc("items_processed")

    if data.get("is_error"):
        context.inc("error_items")
        return None

    if context.get("start_time") is None:
        import time
        context.set("start_time", time.time())

    return data["value"]

# The aggregator is now much simpler. It doesn't need to know about the context,
# as the hook handles the metric printing.
@aggregator_stage(hooks=Hooks(on_stream_end=print_metrics_on_end))
def final_aggregator(iterable):
    """
    An aggregator stage that sums the values from the input stream.
    The on_stream_end hook will print metrics before this function runs.
    """
    yield sum(iterable)


def main():
    data = [
        {"value": 10},
        {"value": 20},
        {"is_error": True},
        {"value": 30},
    ]

    pipeline = process_data | final_aggregator

    results, final_context = pipeline.collect(data)

    print("\n--- Final Result ---")
    print(f"Sum of values: {results[0]}")

    print("\n--- Final Context Object ---")
    print(final_context.to_dict())


if __name__ == "__main__":
    main()
