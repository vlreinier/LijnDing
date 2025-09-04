import pytest
from lijnding.core import Pipeline, stage, ErrorPolicy, aggregator_stage
from tests.helpers.test_runner import run_pipeline


@pytest.mark.parametrize(
    "backend", ["serial", "thread"]
)  # Process/async have extra complexity
@pytest.mark.asyncio
async def test_route_to_stage_policy(backend):
    """
    Tests that the 'route_to_stage' error policy correctly routes failed
    items to a dead-letter stage.
    """
    dead_letter_items = []

    @aggregator_stage
    def dead_letter_handler(items):
        dead_letter_items.extend(list(items))

    error_policy = ErrorPolicy(
        mode="route_to_pipeline", route_to_pipeline=dead_letter_handler
    )

    @stage(backend=backend, error_policy=error_policy)
    def failing_stage(item):
        if not isinstance(item, int):
            raise ValueError("Must be an integer")
        return item * 10

    pipeline = Pipeline([failing_stage])
    data = [1, "two", 3, "four", 5]
    results, _ = await run_pipeline(pipeline, data)

    # Check that the main pipeline only contains results from successful items
    assert sorted(results) == [10, 30, 50]

    # Check that the dead-letter stage received the failed items
    # The dead letter handler is called once per failed item, so the list will be flat
    assert sorted(dead_letter_items) == ["four", "two"]
