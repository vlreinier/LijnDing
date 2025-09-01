import pytest
from lijnding import Pipeline, stage
from tests.helpers.test_runner import run_pipeline, BACKENDS


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
