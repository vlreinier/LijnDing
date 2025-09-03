from __future__ import annotations

from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Iterable, List, Union, AsyncIterator

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage

if TYPE_CHECKING:
    from ..core.context import Context


async def _async_zip(*iterators: AsyncIterator[Any]) -> AsyncIterator[tuple[Any, ...]]:
    """
    Zips multiple async iterators together.
    Stops when the shortest iterator is exhausted.
    """
    if not iterators:
        return

    iterators = [it.__aiter__() for it in iterators]
    while True:
        try:
            yield tuple([await it.__anext__() for it in iterators])
        except StopAsyncIteration:
            return


async def _async_zip_longest(*iterators: AsyncIterator[Any], fillvalue: Any = None) -> AsyncIterator[tuple[Any, ...]]:
    """
    Zips multiple async iterators together.
    Stops when the longest iterator is exhausted, filling missing values with `fillvalue`.
    """
    if not iterators:
        return

    iterators = [it.__aiter__() for it in iterators]
    num_iterators = len(iterators)
    finished = [False] * num_iterators

    while True:
        results = []
        num_finished = 0

        for i, it in enumerate(iterators):
            if finished[i]:
                results.append(fillvalue)
                num_finished += 1
                continue

            try:
                results.append(await it.__anext__())
            except StopAsyncIteration:
                finished[i] = True
                num_finished += 1
                results.append(fillvalue)

        if num_finished == num_iterators:
            break

        yield tuple(results)


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
        @stage(
            name=f"Branch(merge='{merge}')",
            stage_type="itemwise",
            backend="async",
            branch_pipelines=branch_pipelines,
        )
        async def _branch_func_async(context: "Context", item: Any) -> AsyncIterator[Any]:
            branch_iterators = [
                (await p.run_async([item]))[0] for p in branch_pipelines
            ]

            if merge == "concat":
                for it in branch_iterators:
                    async for res in it:
                        yield res
            elif merge == "zip":
                async for res in _async_zip(*branch_iterators):
                    yield res
            elif merge == "zip_longest":
                async for res in _async_zip_longest(*branch_iterators, fillvalue=None):
                    yield res

        return _branch_func_async
    else:
        @stage(
            name=f"Branch(merge='{merge}')",
            stage_type="itemwise",
            branch_pipelines=branch_pipelines,
        )
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
