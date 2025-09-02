from __future__ import annotations

from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Iterable, List, Union, AsyncIterator

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage

if TYPE_CHECKING:
    from ..core.context import Context


def branch(*branches: Union[Stage, "Pipeline"], merge: str = "concat") -> Stage:
    """
    A factory function for creating a Branch component.
    This component creates parallel execution paths in a pipeline.
    It returns a single Stage that can be used in a pipeline.
    """
    if not branches:
        raise ValueError("Branch must have at least one branch.")

    # Convert all provided branches into Pipeline objects
    branch_pipelines: List[Pipeline] = []
    for b in branches:
        if isinstance(b, Stage):
            branch_pipelines.append(Pipeline([b]))
        elif isinstance(b, Pipeline):
            branch_pipelines.append(b)
        else:
            raise TypeError(f"Branch arguments must be Stage or Pipeline, not {type(b)}")

    if merge not in ["concat", "zip", "zip_longest"]:
        raise ValueError(f"Unknown merge strategy: '{merge}'")

    # Determine if any branch requires an async backend
    is_async_branch = any("async" in p._get_required_backend_names() for p in branch_pipelines)

    if is_async_branch:
        @stage(name=f"Branch(merge='{merge}')", stage_type="itemwise", backend="async")
        async def _branch_func_async(context: "Context", item: Any) -> AsyncIterator[Any]:
            branch_iterators = [
                (await p.run_async([item]))[0] for p in branch_pipelines
            ]

            if merge == "concat":
                for it in branch_iterators:
                    async for res in it:
                        yield res
            elif merge == "zip":
                raise NotImplementedError("Async zip merge strategy is not yet implemented.")
            elif merge == "zip_longest":
                raise NotImplementedError("Async zip_longest merge strategy is not yet implemented.")

        return _branch_func_async
    else:
        @stage(name=f"Branch(merge='{merge}')", stage_type="itemwise")
        def _branch_func_sync(context: "Context", item: Any) -> Iterable[Any]:
            branch_iterators = [
                p.run([item], collect=False)[0] for p in branch_pipelines
            ]

            if merge == "concat":
                for it in branch_iterators:
                    yield from it
            elif merge == "zip":
                for zipped_items in zip(*branch_iterators):
                    yield zipped_items
            elif merge == "zip_longest":
                for zipped_items in zip_longest(*branch_iterators, fillvalue=None):
                    yield zipped_items

        return _branch_func_sync
