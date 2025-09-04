import pytest
from lijnding.core import Pipeline, stage
from tests.helpers.test_runner import run_pipeline, BACKENDS


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
