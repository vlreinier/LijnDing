import types
from typing import Any, Iterable


def ensure_iterable(obj: Any) -> Iterable[Any]:
    """
    Ensures that the given object is an iterable.
    If it's a list, tuple, or generator, it's returned as is.
    If it's another type, it's wrapped in a list.
    `None` is treated as an empty list.
    """
    if obj is None:
        return []
    # Pass through lists, tuples, and all kinds of generators
    if isinstance(obj, (list, tuple, types.GeneratorType, types.AsyncGeneratorType)):
        return obj
    # Wrap other types in a list
    return [obj]
