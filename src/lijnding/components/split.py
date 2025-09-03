from __future__ import annotations

from typing import Any, Callable, Iterable, Optional

from ..core.stage import Stage, stage


def split(func: Optional[Callable[[Any], Iterable[Any]]] = None) -> Stage:
    """
    Creates a stage that splits a single input item into multiple output items.

    This is an itemwise stage. For each item it receives, it applies the
    provided function to get an iterable, and then yields each item from
    that iterable.

    Args:
        func: A function that takes one item and returns an iterable.
              If None, the stage assumes the input item is already iterable
              and yields from it.

    Returns:
        A Stage configured to perform the split operation.
    """

    @stage(name="split", stage_type="itemwise")
    def _split_func(item: Any) -> Iterable[Any]:
        """The underlying function for the split stage."""
        if func:
            # If a function is provided, use it to generate the iterable
            yield from func(item)
        elif hasattr(item, "__iter__") and not isinstance(item, (str, bytes, dict)):
            # If no function, assume the item itself is the iterable
            yield from item
        else:
            # Otherwise, just yield the item itself
            yield item

    return _split_func
