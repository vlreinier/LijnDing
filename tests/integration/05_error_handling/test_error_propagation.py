import pytest
from lijnding import Pipeline, stage
from ...helpers.test_runner import run_pipeline, BACKENDS


def top_level_raiser(x):
    raise ValueError("Boom!")


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_error_propagation(backend):
    """Tests that an exception raised in a stage is propagated to the caller."""
    error_stage = stage(top_level_raiser, backend=backend)
    pipeline = Pipeline([error_stage])

    with pytest.raises(ValueError, match="Boom!"):
        await run_pipeline(pipeline, [1, 2, 3])
