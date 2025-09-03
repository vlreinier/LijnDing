import pytest
import itertools
from lijnding.core import Pipeline, Context, stage
from tests.helpers.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend1, backend2", itertools.permutations(BACKENDS, 2))
@pytest.mark.asyncio
async def test_mixed_backends_with_context(backend1, backend2):
    """
    Tests that the context is correctly passed between stages running on
    all combinations of different backends.
    """

    # Define stages inside the test to capture the backend parameters
    @stage(backend=backend1, workers=2)
    def stage1(context: Context, x: int) -> int:
        context.inc(f"{backend1}_counter")
        return x

    @stage(backend=backend2, workers=2)
    def stage2(context: Context, x: int) -> int:
        context.inc(f"{backend2}_counter")
        return x

    pipeline = stage1 | stage2

    input_data = list(range(10))
    # We need to use the run_pipeline helper to handle both sync and async backends
    _, context = await run_pipeline(pipeline, input_data)

    assert context.get(f"{backend1}_counter") == 10
    assert context.get(f"{backend2}_counter") == 10
