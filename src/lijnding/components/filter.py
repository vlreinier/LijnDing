"""
This module provides the `filter_` component, which is used to selectively
keep or discard items from a pipeline stream based on a condition.
"""

from __future__ import annotations
from typing import Any, Callable, TYPE_CHECKING, Generator
from ..core.stage import Stage, stage

if TYPE_CHECKING:
    from ..core.context import Context


def filter_(
    condition: Callable[..., bool], *, name: str = "filter", **stage_kwargs
) -> Stage:
    """
    Creates a stage that filters items from a stream based on a condition.

    Args:
        condition: A callable that returns True for items to keep.
        name: An optional name for the stage.
        **stage_kwargs: Additional keyword arguments for the @stage decorator.

    Returns:
        A Stage that yields items for which the condition is true.
    """

    @stage(name=name, **stage_kwargs)
    def _filter_func(context: Context, item: Any) -> Generator[Any, None, None]:
        if condition(item):
            yield item

    return _filter_func
