"""
This module provides the `map_values` component, which is a fundamental
building block for applying a 1-to-1 transformation to each item in a stream.
"""
from __future__ import annotations

from typing import Any, Callable

from ..core.stage import Stage, stage


def map_values(func: Callable[[Any], Any]) -> Stage:
    """
    Creates a stage that applies a function to each item in the stream.

    This is an itemwise stage that performs a 1-to-1 transformation on items.

    Args:
        func: The function to apply to each item.

    Returns:
        A Stage configured to perform the mapping operation.
    """

    # Try to get a good name for the stage from the function itself
    name = getattr(func, "__name__", "map")
    if name == "<lambda>":
        name = "map"

    @stage(name=name, stage_type="itemwise")
    def _map_func(item: Any) -> Any:
        return func(item)

    return _map_func
