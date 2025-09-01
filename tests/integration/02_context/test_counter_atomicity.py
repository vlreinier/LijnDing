import pytest
from lijnding import Pipeline, Context, stage
from ...helpers.test_runner import run_pipeline, BACKENDS


def top_level_context_user(context: Context, x: int):
    return x + context.inc("counter")


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_context_counter_atomicity(backend):
    """
    Tests that the context's `inc` method is atomic across all backends,
    preventing the race condition that was previously fixed.
    """
    # For concurrent backends, use multiple workers to test atomicity.
    workers = 4 if backend in ["threading", "process"] else 1

    context_stage = stage(top_level_context_user, backend=backend, workers=workers)
    pipeline = Pipeline([context_stage])

    input_data = list(range(10))
    results, context = await run_pipeline(pipeline, input_data)

    # Check that the counter was incremented for every item
    assert context.get("counter") == 10

    # Check that the results are correct. For concurrent backends, the order
    # of execution is not guaranteed. This means we can't assert the exact
    # list of results. However, we can assert that the sum of the results
    # is correct, which proves that the logic was applied to every item
    # correctly, just not in a predictable order.
    # Sum of inputs: sum(0..9) = 45
    # Sum of counter increments: sum(1..10) = 55
    # Total sum = 45 + 55 = 100
    expected_sum = sum(i + (i + 1) for i in input_data)
    assert sum(results) == expected_sum
