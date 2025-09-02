import pytest
from lijnding import Pipeline, stage, branch
from tests.helpers.test_runner import run_pipeline, BACKENDS


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_branch_fan_out_concat(backend):
    """Tests the Branch component's fan-out and concat merge strategy."""
    # Define two parallel branches
    add_ten = Pipeline([stage(lambda x: x + 10, backend=backend)])
    times_ten = Pipeline([stage(lambda x: x * 10, backend=backend)])

    # Create a branch that sends each item to both pipelines
    branch_stage = branch(add_ten, times_ten, merge="concat")
    pipeline = Pipeline([branch_stage])

    data = [1, 2]
    results, _ = await run_pipeline(pipeline, data)

    # For input 1, results are [11, 10]. For input 2, results are [12, 20].
    # With 'concat' merge, the final output is a flat list.
    # Order is not guaranteed for concurrent backends.
    assert sorted(results) == [10, 11, 12, 20]
