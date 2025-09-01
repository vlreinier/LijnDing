from __future__ import annotations

from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Iterable, List, Union

from typing import TYPE_CHECKING, Any, Iterable, List, Union

from ..core.stage import Stage, stage

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.pipeline import Pipeline


def branch(*branches: Union[Stage, "Pipeline", "Branch"], merge: str = "concat") -> "Branch":
    """
    A factory function for creating a Branch component.
    This is the recommended, user-facing way to create a branch.
    """
    return Branch(*branches, merge=merge)


class Branch:
    """
    A component for creating parallel execution paths in a pipeline.
    It is recommended to use the lowercase `branch()` factory function instead
    of constructing this class directly.
    """

    def __init__(self, *branches: Union[Stage, "Pipeline"], merge: str = "concat"):
        # Local import to prevent circular dependency
        from ..core.pipeline import Pipeline

        if not branches:
            raise ValueError("Branch must have at least one branch.")

        self.branches: List[Pipeline] = []
        for b in branches:
            if isinstance(b, Stage):
                self.branches.append(Pipeline([b]))
            elif isinstance(b, Pipeline):
                self.branches.append(b)
            else:
                raise TypeError(f"Branch arguments must be Stage or Pipeline, not {type(b)}")

        if merge not in ["concat", "zip", "zip_longest"]:
            raise ValueError(f"Unknown merge strategy: '{merge}'")
        self.merge = merge

    def to_stage(self) -> Stage:
        """
        Converts the Branch configuration into an executable Stage.
        """
        is_async_branch = any("async" in p._get_required_backend_names() for p in self.branches)

        if is_async_branch:
            @stage(name=f"Branch(merge='{self.merge}')", stage_type="itemwise")
            async def _branch_func_async(context: "Context", item: Any) -> AsyncIterator[Any]:
                branch_iterators = [
                    (await branch.run_async([item]))[0] for branch in self.branches
                ]

                if self.merge == "concat":
                    for it in branch_iterators:
                        async for res in it:
                            yield res
                elif self.merge == "zip":
                    raise NotImplementedError("Async zip merge strategy is not yet implemented.")
                elif self.merge == "zip_longest":
                    raise NotImplementedError("Async zip_longest merge strategy is not yet implemented.")

            return _branch_func_async
        else:
            @stage(name=f"Branch(merge='{self.merge}')", stage_type="itemwise")
            def _branch_func_sync(context: "Context", item: Any) -> Iterable[Any]:
                branch_iterators = [
                    branch.run([item], collect=False)[0] for branch in self.branches
                ]

                if self.merge == "concat":
                    for it in branch_iterators:
                        yield from it

                elif self.merge == "zip":
                    for zipped_items in zip(*branch_iterators):
                        yield zipped_items

                elif self.merge == "zip_longest":
                    for zipped_items in zip_longest(*branch_iterators, fillvalue=None):
                        yield zipped_items

            return _branch_func_sync
