import pytest
from typing import Any, List, Union, Iterable

from lijnding import stage, Pipeline
from lijnding.core.errors import PipelineConnectionError


def test_simple_type_match():
    @stage
    def to_int(x: str) -> int:
        return int(x)

    @stage
    def add_one(x: int) -> int:
        return x + 1

    pipeline = to_int | add_one
    assert isinstance(pipeline, Pipeline)
    results, _ = pipeline.run(["1", "2", "3"], collect=True)
    assert results == [2, 3, 4]


def test_simple_type_mismatch():
    @stage
    def to_int(x: str) -> int:
        return int(x)

    @stage
    def to_upper(x: str) -> str:
        return x.upper()

    with pytest.raises(PipelineConnectionError) as excinfo:
        _ = to_int | to_upper
    assert "Type mismatch between stages" in str(excinfo.value)
    assert "Output type of 'to_int': <class 'int'>" in str(excinfo.value)
    assert "Input type of 'to_upper': <class 'str'>" in str(excinfo.value)


def test_any_input_type():
    @stage
    def to_int(x: str) -> int:
        return int(x)

    @stage
    def print_any(x: Any) -> Any:
        print(x)
        return x

    pipeline = to_int | print_any
    assert isinstance(pipeline, Pipeline)
    results, _ = pipeline.run(["1", "2"], collect=True)
    assert results == [1, 2]


def test_any_output_type():
    @stage
    def identity(x: str) -> Any:
        return x

    @stage
    def to_upper(x: str) -> str:
        return x.upper()

    pipeline = identity | to_upper
    assert isinstance(pipeline, Pipeline)
    results, _ = pipeline.run(["a", "b"], collect=True)
    assert results == ["A", "B"]


def test_union_input_type():
    @stage
    def to_int_or_str(x: str) -> Union[int, str]:
        if x.isdigit():
            return int(x)
        return x

    @stage
    def process_item(x: Union[int, str]) -> str:
        return f"processed: {x}"

    pipeline = to_int_or_str | process_item
    assert isinstance(pipeline, Pipeline)
    results, _ = pipeline.run(["1", "a"], collect=True)
    assert results == ["processed: 1", "processed: a"]


def test_union_output_type_compatible():
    @stage
    def to_int_or_str(x: str) -> Union[int, str]:
        if x.isdigit():
            return int(x)
        return x

    @stage
    def process_item(x: object) -> str:
        return f"processed: {x}"

    pipeline = to_int_or_str | process_item
    assert isinstance(pipeline, Pipeline)


def test_union_output_type_incompatible():
    @stage
    def to_int_or_str(x: str) -> Union[int, str]:
        if x.isdigit():
            return int(x)
        return x

    @stage
    def process_item(x: float) -> str:
        return f"processed: {x}"

    with pytest.raises(PipelineConnectionError):
        _ = to_int_or_str | process_item


from lijnding import stage, Pipeline, aggregator_stage

def test_iterable_match():
    @stage
    def to_list_of_ints(x: str) -> List[int]:
        return [int(c) for c in x]

    @aggregator_stage
    def process_list(x: List[int]) -> int:
        return sum(x)

    pipeline = to_list_of_ints | process_list
    assert isinstance(pipeline, Pipeline)
    results, _ = pipeline.run(["123", "45"], collect=True)
    assert results == [15]


def test_iterable_mismatch():
    @stage
    def to_list_of_ints(x: str) -> List[int]:
        return [int(c) for c in x]

    @stage
    def process_string(x: str) -> int:
        return len(x)

    with pytest.raises(PipelineConnectionError):
        _ = to_list_of_ints | process_string


def test_explicit_type_override_connects():
    """
    Tests that a pipeline connects when types are explicitly overridden,
    even if the underlying function annotations don't match.
    """
    @stage(output_type=str)
    def returns_int_but_lies(x: str) -> int:
        return int(x)

    @stage
    def expects_string(x: str) -> str:
        return x + "!"

    # This should connect without a PipelineConnectionError because of the override
    pipeline = returns_int_but_lies | expects_string
    assert isinstance(pipeline, Pipeline)

    # The test also verifies that it fails at runtime with a TypeError,
    # because the actual data type is incorrect.
    with pytest.raises(TypeError):
        pipeline.run(["1", "2"], collect=True)


from lijnding.typing.checker import check_instance

@pytest.mark.parametrize("obj, type_hint, expected", [
    (1, int, True),
    ("s", int, False),
    ([1, 2], List[int], True),
    ([1, "s"], List[int], False),
    ({"a": 1}, dict[str, int], True),
    ({"a": "s"}, dict[str, int], False),
    ((1, "s"), tuple[int, str], True),
    ((1, 2), tuple[int, str], False),
    (1, Union[int, str], True),
    (1.0, Union[int, str], False),
    (1, Any, True),
])
def test_check_instance(obj, type_hint, expected):
    assert check_instance(obj, type_hint) == expected
