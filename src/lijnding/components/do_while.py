from __future__ import annotations

from typing import Any, Callable, Union, Iterable, AsyncIterator, List

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage
from ..core.context import Context


def do_while(condition: Callable[[Any], bool], body: Union[Stage, Pipeline]) -> Stage:
    """
    A factory function for creating a Do-While component.

    This component executes a 'body' pipeline at least once, and then
    repeatedly as long as a 'condition' is true.

    The body pipeline must take a single item as input and produce a single
    item as output. This output is then fed back into the condition and the
    next iteration of the body.

    Args:
        condition: A callable that takes an item and returns True if the
                   loop should continue, False otherwise.
        body: The Stage or Pipeline to execute in each iteration.

    Returns:
        A Stage that encapsulates the do-while loop logic.
    """
    if isinstance(body, Stage):
        body_pipeline = Pipeline([body])
    elif isinstance(body, Pipeline):
        body_pipeline = body
    else:
        raise TypeError(f"Body must be a Stage or Pipeline, not {type(body)}")

    is_async_body = "async" in body_pipeline._get_required_backend_names()

    if is_async_body:
        @stage(name="DoWhile", stage_type="itemwise", backend="async")
        async def _do_while_func_async(context: Context, item: Any) -> AsyncIterator[Any]:
            current_item = item
            while True:
                stream, _ = await body_pipeline.run_async(data=[current_item])
                results: List[Any] = [res async for res in stream]

                if len(results) != 1:
                    raise ValueError(
                        f"The body of a do_while loop must produce exactly one item, "
                        f"but it produced {len(results)} items."
                    )
                current_item = results[0]
                if not condition(current_item):
                    break
            yield current_item

        return _do_while_func_async
    else:
        @stage(name="DoWhile", stage_type="itemwise")
        def _do_while_func_sync(context: Context, item: Any) -> Iterable[Any]:
            current_item = item
            while True:
                results, _ = body_pipeline.collect(data=[current_item])

                if len(results) != 1:
                    raise ValueError(
                        f"The body of a do_while loop must produce exactly one item, "
                        f"but it produced {len(results)} items."
                    )
                current_item = results[0]
                if not condition(current_item):
                    break
            yield current_item

        return _do_while_func_sync
