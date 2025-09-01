import pytest
from lijnding import Pipeline, stage
from tests.utils.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_identity_pipeline(backend):
    """Tests that a simple single-stage pipeline returns the data it was given."""
    identity_stage = stage(lambda x: x, backend=backend)
    pipeline = Pipeline([identity_stage])
    data = [1, 5, 2, 4, 3]

    results, _ = await run_pipeline(pipeline, data)

    assert sorted(results) == sorted(data)
