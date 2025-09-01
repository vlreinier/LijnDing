import pytest
from lijnding import Pipeline, stage
from ...helpers.test_runner import run_pipeline


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
