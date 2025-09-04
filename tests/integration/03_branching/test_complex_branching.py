import pytest
from lijnding.core import Pipeline, stage
from lijnding.components.branch import branch


@stage
def add_one(x):
    return x + 1


@stage
def multiply_by_two(x):
    return x * 2


@pytest.mark.parametrize("backend", ["serial", "thread", "process", "async"])
def test_branch_with_nested_pipeline(backend):
    """
    Tests a pipeline where one of the branches is a nested pipeline.
    """
    # Create stages with the specified backend
    add_one_stage = stage(add_one.func, backend=backend)
    multiply_by_two_stage = stage(multiply_by_two.func, backend=backend)

    nested_pipeline = Pipeline([multiply_by_two_stage, add_one_stage])

    pipeline = Pipeline([branch(add_one_stage, nested_pipeline, merge="concat")])

    data = [1, 2, 3]

    if backend == "async":
        import asyncio

        async def run_test():
            output_data, _ = await pipeline.run_async(data)
            return [i async for i in output_data]

        results = asyncio.run(run_test())
    else:
        results, _ = pipeline.collect(data)

    # Branch 1 (add_one): [2, 3, 4]
    # Branch 2 (nested_pipeline): 1*2+1=3, 2*2+1=5, 3*2+1=7 -> [3, 5, 7]
    # Concat: [2, 3, 4, 3, 5, 7]
    assert sorted(results) == [2, 3, 3, 4, 5, 7]
