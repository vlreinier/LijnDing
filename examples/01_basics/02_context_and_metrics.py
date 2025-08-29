"""
An example demonstrating the use of the Context object to share state
and collect metrics across a pipeline.
"""
from lijnding import stage, Context

@stage
def process_data(context: Context, data: dict):
    """
    A stage that processes data and uses the context to track metrics.
    """
    # The context is automatically injected because it's in the function signature.
    context.inc("items_processed")

    if data.get("is_error"):
        context.inc("error_items")
        return None # Skip this item

    # We can also store arbitrary data in the context
    if context.get("start_time") is None:
        import time
        context.set("start_time", time.time())

    return data["value"]

@stage
def final_aggregator(context: Context, iterable):
    """
    An aggregator stage that can access the final context.
    """
    # This stage receives the context shared by the whole pipeline.
    total_items = context.get("items_processed")
    error_items = context.get("error_items")

    print("\n--- Metrics from Context ---")
    print(f"Total items processed: {total_items}")
    print(f"Items with errors: {error_items}")

    # The iterable is still passed, so we can process it.
    yield sum(iterable)


def main():
    data = [
        {"value": 10},
        {"value": 20},
        {"is_error": True},
        {"value": 30},
    ]

    # The context object is created and managed by the pipeline automatically.
    pipeline = process_data | final_aggregator

    results, final_context = pipeline.collect(data)

    print("\n--- Final Result ---")
    print(f"Sum of values: {results[0]}")

    print("\n--- Final Context Object ---")
    print(final_context.to_dict())


if __name__ == "__main__":
    main()
