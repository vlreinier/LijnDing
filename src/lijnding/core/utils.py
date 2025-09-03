import types
from typing import Any, Iterable


def ensure_iterable(obj: Any) -> Iterable[Any]:
    """
    Ensures that the given object is an iterable.
    If it's a list or a generator, it's returned as is.
    If it's another type (including a tuple), it's wrapped in a list.
    `None` is treated as an empty list.
    """
    if obj is None:
        return []
    # Pass through lists and all kinds of generators
    if isinstance(obj, (list, types.GeneratorType, types.AsyncGeneratorType)):
        return obj
    # Wrap other types (including tuples) in a list to treat them as a single item
    return [obj]
