import pytest
import asyncio
from typing import Iterable, Any, Tuple, List
from lijnding import Pipeline, Context, stage
from lijnding.core.stage import Stage
from lijnding.components.branch import Branch

# A list of all backends to be tested
BACKENDS = ["serial", "thread", "process", "async"]


# Helper function to run sync or async pipelines transparently
async def run_pipeline(pipeline: Pipeline, data: Iterable[Any]) -> Tuple[List[Any], Context]:
    """
    Runs a pipeline and collects its results, automatically handling sync and async backends.
    """
    if "async" in pipeline._get_required_backend_names():
        stream, context = await pipeline.run_async(data)
        results = [item async for item in stream]
        return results, context
    else:
        # This is a blocking call, but it's what's needed for sync backends.
        # The `pytest-asyncio` runner will handle running this in the event loop.
        return pipeline.collect(data)

# Top-level functions needed for the 'process' backend
def top_level_double(x):
    return x * 2

def top_level_context_user(context: Context, x: int):
    return x + context.inc("counter")

def top_level_raiser(x):
    raise ValueError("Boom!")

def top_level_is_even(x):
    return x % 2 == 0


# --- Test Scenarios ---


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_identity_pipeline(backend):
    """Tests that a simple single-stage pipeline returns the data it was given."""
    identity_stage = stage(lambda x: x, backend=backend)
    pipeline = Pipeline([identity_stage])
    data = [1, 5, 2, 4, 3]

    results, _ = await run_pipeline(pipeline, data)

    assert sorted(results) == sorted(data)


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

    # Check that the results are correct.
    expected_results = [i + (i + 1) for i in input_data]
    assert sorted(results) == sorted(expected_results)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_multi_stage_transformation(backend):
    """Tests a pipeline with multiple simple transformation stages."""
    add_one = stage(lambda x: x + 1, backend=backend)
    times_two = stage(lambda x: x * 2, backend=backend)

    pipeline = Pipeline([add_one, times_two])
    data = [1, 2, 3]
    results, _ = await run_pipeline(pipeline, data)

    # x=1 -> 1+1=2 -> 2*2=4
    # x=2 -> 2+1=3 -> 3*2=6
    # x=3 -> 3+1=4 -> 4*2=8
    assert sorted(results) == [4, 6, 8]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_error_propagation(backend):
    """Tests that an exception raised in a stage is propagated to the caller."""
    error_stage = stage(top_level_raiser, backend=backend)
    pipeline = Pipeline([error_stage])

    with pytest.raises(ValueError, match="Boom!"):
        await run_pipeline(pipeline, [1, 2, 3])


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_empty_input(backend):
    """Tests that the pipeline runs correctly with an empty list of input data."""
    s = stage(lambda x: x * 2, backend=backend)
    pipeline = Pipeline([s])
    results, _ = await run_pipeline(pipeline, [])
    assert results == []


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_basic_aggregator(backend):
    """Tests a simple aggregator stage."""
    sum_stage = stage(sum, stage_type="aggregator", backend=backend)
    pipeline = Pipeline([sum_stage])
    data = [1, 2, 3, 4, 5]
    results, _ = await run_pipeline(pipeline, data)
    assert results == [15]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_mixed_pipeline_itemwise_to_aggregator(backend):
    """Tests a pipeline with an itemwise stage followed by an aggregator."""
    double = stage(lambda x: x * 2, backend=backend)
    summer = stage(sum, stage_type="aggregator", backend=backend)

    pipeline = Pipeline([double, summer])
    data = [1, 2, 3]  # -> [2, 4, 6]
    results, _ = await run_pipeline(pipeline, data)
    assert results == [12]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_branch_fan_out_concat(backend):
    """Tests the Branch component's fan-out and concat merge strategy."""
    # Define two parallel branches
    add_ten = Pipeline([stage(lambda x: x + 10, backend=backend)])
    times_ten = Pipeline([stage(lambda x: x * 10, backend=backend)])

    # Create a branch that sends each item to both pipelines
    branch_component = Branch(add_ten, times_ten, merge="concat")
    pipeline = Pipeline([branch_component.to_stage()])

    data = [1, 2]
    results, _ = await run_pipeline(pipeline, data)

    # For input 1, results are [11, 10]. For input 2, results are [12, 20].
    # With 'concat' merge, the final output is a flat list.
    # Order is not guaranteed for concurrent backends.
    assert sorted(results) == [10, 11, 12, 20]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_context_data_propagation(backend):
    """Tests that data can be passed between stages via the context."""
    def writer(context, item):
        context.set(f"key_{item}", item * 2)
        return item

    def reader(context, item):
        return context.get(f"key_{item}")

    writer_stage = stage(writer, backend=backend)
    reader_stage = stage(reader, backend=backend)

    pipeline = Pipeline([writer_stage, reader_stage])
    data = [1, 2, 3]
    results, _ = await run_pipeline(pipeline, data)

    assert sorted(results) == [2, 4, 6]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_nested_pipeline(backend):
    """Tests a pipeline that is nested inside another pipeline as a stage."""
    # This is the inner pipeline
    inner_pipeline = Pipeline([
        stage(lambda x: x + 1, backend=backend),
        stage(lambda x: x * 2, backend=backend),
    ])

    # This is the outer pipeline
    outer_pipeline = Pipeline([
        stage(lambda x: x * 10, backend=backend),
        inner_pipeline.to_stage(), # The inner pipeline is converted to a stage
        stage(lambda x: x - 1, backend=backend),
    ])

    data = [1, 2, 3]
    # x=1: 1*10=10 -> (10+1)*2=22 -> 22-1=21
    # x=2: 2*10=20 -> (20+1)*2=42 -> 42-1=41
    # x=3: 3*10=30 -> (30+1)*2=62 -> 62-1=61
    results, _ = await run_pipeline(outer_pipeline, data)
    assert sorted(results) == [21, 41, 61]


@pytest.mark.parametrize("backend", ["serial", "thread", "async"])
@pytest.mark.asyncio
async def test_source_stage(backend):
    """Tests a pipeline that starts with a source stage."""
    # The 'process' backend cannot serialize a generator function defined
    # inside another function, so it is excluded from this test.

    @stage(stage_type="source", backend=backend)
    def my_generator():
        yield 1
        yield 2
        yield 3

    pipeline = Pipeline([my_generator, stage(lambda x: x * 10, backend=backend)])

    # Source stages don't take input data
    results, _ = await run_pipeline(pipeline, [])
    assert sorted(results) == [10, 20, 30]
