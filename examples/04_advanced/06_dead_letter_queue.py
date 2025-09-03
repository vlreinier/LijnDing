"""
An advanced example demonstrating how to use the 'route_to_stage' error
policy to create a dead-letter queue for handling failed items.
"""
from lijnding.core import stage, aggregator_stage, ErrorPolicy, Pipeline

# --- Dead-letter Stage Definition ---

@aggregator_stage
def dead_letter_handler(failed_items):
    """
    This stage will process items that fail in the main pipeline.
    In a real application, this might write to a log file, a database,
    or a separate message queue for later inspection.
    """
    print("\n--- Dead-Letter Handler Activated ---")
    all_items = list(failed_items)
    print(f"Received {len(all_items)} failed item(s).")
    for item in all_items:
        print(f"  - Failed item: {item!r}")
    yield all_items


# --- Main Pipeline Stage Definitions ---

# Create an ErrorPolicy that routes failures to our handler stage.
# Note that the handler can be a Stage or a full Pipeline.
dead_letter_policy = ErrorPolicy(
    mode="route_to_stage",
    route_to=dead_letter_handler
)

# Apply this policy to a stage that is expected to fail.
@stage(error_policy=dead_letter_policy)
def process_number(item: any) -> int:
    """
    A stage that processes numbers but will fail on non-integer types.
    """
    if not isinstance(item, int):
        raise TypeError(f"Input must be an integer, but got {type(item).__name__}")
    return item * 10


@stage
def add_one(number: int) -> int:
    return number + 1


def main():
    """Builds and runs the pipeline."""
    data = [1, 2, "three", 4, "five"]

    # The main pipeline will process numbers. When `process_number` fails on
    # a string, the item will be sent to `dead_letter_handler` instead of
    # crashing the pipeline.
    main_pipeline = process_number | add_one

    print("--- Running Main Pipeline ---")
    results, context = main_pipeline.collect(data)
    print("--- Main Pipeline Finished ---")

    print(f"\nSuccessfully processed results: {results}")


if __name__ == "__main__":
    main()
