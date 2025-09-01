import pytest
from lijnding import Pipeline, stage
from ...helpers.test_runner import run_pipeline, BACKENDS


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
