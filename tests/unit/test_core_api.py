import pytest
import asyncio
from lijnding.core.pipeline import Pipeline
from lijnding.core.stage import Stage, stage
from lijnding.core.context import Context
from lijnding.core.errors import ErrorPolicy

# --- Test Stages ---

@stage
def add_one(x: int) -> int:
    return x + 1

@stage
def to_string(x: int) -> str:
    return str(x)

@stage
def context_incrementer(context: Context, x: int) -> int:
    context.inc("my_counter")
    return x

@stage(backend="async")
async def add_one_async(x: int) -> int:
    await asyncio.sleep(0.001)
    return x + 1

# --- Core Tests ---

def test_pipeline_creation():
    p = Pipeline()
    assert isinstance(p, Pipeline)
    assert p.stages == []

def test_stage_creation():
    s = Stage(lambda x: x * 2)
    assert isinstance(s, Stage)

def test_decorator_creation():
    assert isinstance(add_one, Stage)

def test_pipeline_composition():
    p = Pipeline() | add_one | to_string
    assert len(p.stages) == 2
    assert p.stages[0].name == "add_one"
    assert p.stages[1].name == "to_string"

def test_simple_pipeline_execution():
    pipeline = Pipeline() | add_one | add_one
    data = [1, 2, 3]
    results, _ = pipeline.collect(data)
    assert results == [3, 4, 5]

def test_context_creation_and_inc():
    ctx = Context()
    ctx.inc("test_key")
    ctx.inc("test_key", 5)
    assert ctx.get("test_key") == 6

def test_context_is_used_in_pipeline():
    pipeline = Pipeline() | context_incrementer
    data = [1, 2, 3]
    _, context = pipeline.collect(data)
    assert context.get("my_counter") == 3

def test_error_policy_validation():
    ErrorPolicy(mode="fail")
    ErrorPolicy(mode="skip")
    ErrorPolicy(mode="retry", retries=1)

    with pytest.raises(ValueError):
        ErrorPolicy(mode="invalid_mode")

    with pytest.raises(ValueError):
        ErrorPolicy(mode="retry", retries=0)

def test_empty_pipeline():
    pipeline = Pipeline()
    data = [1, 2, 3]
    results, _ = pipeline.collect(data)
    assert results == data


def test_stage_direct_execution():
    """Tests that a single stage can be executed directly using .collect()."""
    data = [1, 2, 3]
    results, context = add_one.collect(data)
    assert results == [2, 3, 4]
    assert isinstance(context, Context)


@pytest.mark.asyncio
async def test_stage_direct_async_execution():
    """Tests that a single async stage can be executed directly using .run_async()."""
    data = [1, 2, 3]
    stream, context = await add_one_async.run_async(data)
    results = [item async for item in stream]
    assert results == [2, 3, 4]
    assert isinstance(context, Context)


def test_pipeline_fluent_interface():
    """Tests that a pipeline can be built using the fluent .add() interface."""
    pipeline = Pipeline().add(add_one).add(to_string)
    assert len(pipeline.stages) == 2
    assert pipeline.stages[0].name == "add_one"
    assert pipeline.stages[1].name == "to_string"

    data = [1, 2, 3]
    results, _ = pipeline.collect(data)
    assert results == ["2", "3", "4"]


def test_stage_informative_error_message():
    """Tests that calling a Pipeline-only method on a Stage gives a helpful error."""
    with pytest.raises(AttributeError) as excinfo:
        add_one.to_stage()  # to_stage is a method on Pipeline, not Stage

    expected_message = (
        "'Stage' object has no attribute 'to_stage'. "
        "Did you mean to wrap it in a Pipeline first? "
        "e.g., Pipeline([add_one]).to_stage(...)"
    )
    assert str(excinfo.value) == expected_message
