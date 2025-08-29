import pytest
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
