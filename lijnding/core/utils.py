from typing import Any, Iterable

def ensure_iterable(x: Any) -> Iterable[Any]:
    """
    Ensures the input is a non-string iterable.

    - If x is None, returns an empty list.
    - If x is already a non-string iterable, returns x.
    - Otherwise, wraps x in a list.
    """
    if x is None:
        return []
    if hasattr(x, "__iter__") and not isinstance(x, (str, bytes, dict)):
        return x
    return [x]
