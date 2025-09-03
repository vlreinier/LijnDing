from __future__ import annotations

from typing import Any, Callable, Iterable, Optional
import functools

from ..core.stage import Stage, aggregator_stage


def reduce_(func: Callable[[Any, Any], Any], initializer: Optional[Any] = None) -> Stage:
    """
    Creates a stage that reduces an entire input stream to a single value.

    This is an aggregator stage. It consumes all items from the input and
    applies a reduction function, yielding a single result. It is a wrapper
    around Python's `functools.reduce`.

    Args:
        func: The two-argument reduction function.
        initializer: The optional starting value for the reduction.

    Returns:
        A Stage configured to perform the reduction.
    """

    @aggregator_stage(name="reduce")
    def _reduce_func(iterable: Iterable[Any]) -> Any:
        """The underlying function for the reduce stage."""
        iterator = iter(iterable)

        if initializer is None:
            try:
                # Get the first item as the initializer
                initial_value = next(iterator)
            except StopIteration:
                # If the iterable is empty and there's no initializer,
                # yield nothing.
                return None
        else:
            initial_value = initializer

        # Perform the reduction
        result = functools.reduce(func, iterator, initial_value)
        yield result

    return _reduce_func
