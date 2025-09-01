from typing import Callable, Any, Generator

from ..core.stage import stage, Stage


def filter_(predicate: Callable[[Any], bool], *, name: str = "filter") -> Stage:
    """
    Creates a pipeline stage that filters items based on a predicate.

    This component yields an item only if the predicate function returns True.

    :param predicate: A callable that accepts an item and returns a boolean.
                      If True, the item is passed through; otherwise, it is discarded.
    :param name: An optional name for the stage.
    :return: A new `Stage` that performs the filtering logic.
    """
    @stage(name=name, stage_type="itemwise")
    def _filter_stage(item: Any) -> Generator[Any, None, None]:
        """The actual stage function that applies the predicate."""
        if predicate(item):
            yield item

    return _filter_stage
