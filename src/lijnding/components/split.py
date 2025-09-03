"""
This module provides the `split` component, which is used to transform a
single item into a stream of multiple items.
"""
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
        if func:
            # If a function is provided, call it and yield from the resulting iterable.
            yield from func(item)
        elif hasattr(item, "__iter__") and not isinstance(item, (str, bytes, dict)):
            # If no function is given, check if the item is a common iterable type
            # (but not a string, bytes, or dict, which are technically iterable
            # but usually treated as single items).
            yield from item
        else:
            # If the item is not a standard iterable or a function was not provided,
            # yield the item itself as a single-element stream.
            yield item

    return _split_func
