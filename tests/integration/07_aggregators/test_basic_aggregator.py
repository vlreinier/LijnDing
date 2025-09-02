import pytest
from lijnding import Pipeline, aggregator_stage, stage, Hooks, Context
from tests.helpers.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_basic_aggregator(backend):
    """Tests a simple aggregator stage."""
    sum_stage = aggregator_stage(sum, backend=backend)
    pipeline = Pipeline([sum_stage])
    data = [1, 2, 3, 4, 5]
    results, _ = await run_pipeline(pipeline, data)
    assert results == [15]


@pytest.mark.parametrize("backend", ["serial", "thread"]) # Process backend has issues with closures
@pytest.mark.asyncio
async def test_aggregator_on_stream_end_hook(backend):
    """Tests that the on_stream_end hook is called correctly for aggregators."""
    # This list will be used to check the order of events
    event_log = []

    def my_hook(context: Context):
        # The hook should be called after the upstream stage is done
        assert context.get("upstream_done") is True
        event_log.append("hook_called")

    @stage(backend=backend)
    def upstream_stage(context: Context, item: int):
        context.set("upstream_done", True)
        return item * 2

    @aggregator_stage(backend=backend, hooks=Hooks(on_stream_end=my_hook))
    def my_aggregator(items):
        # The main aggregator logic runs after the hook
        event_log.append("aggregator_called")
        yield sum(items)

    pipeline = upstream_stage | my_aggregator
    results, _ = await run_pipeline(pipeline, [1, 2, 3])

    assert results == [12]
    # Check that the hook was called before the aggregator's main logic
    assert event_log == ["hook_called", "aggregator_called"]
