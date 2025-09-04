"""
An example demonstrating the use of ErrorPolicy for robust pipelines.
"""

from lijnding.core import Pipeline, stage, ErrorPolicy


class CustomError(Exception):
    """A custom exception for demonstration purposes."""

    pass


# --- A stage that can fail ---
@stage
def might_fail(item: dict):
    """
    This stage will fail if the input item's 'value' is 0.
    """
    if item.get("value") == 0:
        raise CustomError(f"Failing on item: {item}")
    return item


# --- A stage that can be retried ---
RETRY_ATTEMPTS = 0


@stage(error_policy=ErrorPolicy(mode="retry", retries=3, backoff=0.5))
def needs_retry(item: dict):
    """
    This stage will fail a few times before succeeding.
    """
    global RETRY_ATTEMPTS

    if item.get("id") == "retry_me" and RETRY_ATTEMPTS < 2:
        RETRY_ATTEMPTS += 1
        print(f"Attempt {RETRY_ATTEMPTS}: Failing on '{item['id']}', will retry...")
        raise ConnectionError("Simulated network failure")

    print(f"Successfully processed '{item['id']}' after {RETRY_ATTEMPTS} retries.")
    return item


def main():
    data = [
        {"id": "a", "value": 1},
        {"id": "b", "value": 0},  # This one will fail
        {"id": "c", "value": 2},
    ]

    # --- Example 1: Default 'fail' policy ---
    print("--- Running with default 'fail' policy ---")
    fail_pipeline = Pipeline() | might_fail
    try:
        fail_pipeline.collect(data)
    except CustomError as e:
        print(f"Pipeline failed as expected: {e}")
    print("-" * 20)

    # --- Example 2: 'skip' policy ---
    print("\n--- Running with 'skip' policy ---")
    # We create a new stage instance with a different error policy
    skipping_stage = stage(error_policy=ErrorPolicy(mode="skip"))(might_fail.func)
    skip_pipeline = Pipeline() | skipping_stage

    results, _ = skip_pipeline.collect(data)
    print("Input:", [d["id"] for d in data])
    print("Result:", [r["id"] for r in results])
    # The error on item 'b' should be recorded in the stage's metrics
    print("Metrics for skipping_stage:", skip_pipeline.stages[0].metrics)
    print("-" * 20)

    # --- Example 3: 'retry' policy ---
    print("\n--- Running with 'retry' policy ---")
    retry_data = [
        {"id": "first"},
        {"id": "retry_me"},  # This will fail twice before succeeding
        {"id": "third"},
    ]
    retry_pipeline = Pipeline() | needs_retry
    results_retry, _ = retry_pipeline.collect(retry_data)
    print("Result:", [r["id"] for r in results_retry])
    print("-" * 20)


if __name__ == "__main__":
    main()
