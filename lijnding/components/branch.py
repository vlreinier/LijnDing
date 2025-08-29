from __future__ import annotations

from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Iterable, List, Union

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage

if TYPE_CHECKING:
    from ..core.context import Context


class Branch:
    """
    A component for creating parallel execution paths in a pipeline.

    Branch takes multiple sub-pipelines (branches) and runs them all on the
    same input item. It then merges the results from these branches back into
    a single stream according to a specified merge strategy.
    """

    def __init__(self, *branches: Union[Stage, Pipeline], merge: str = "concat"):
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
        The returned stage is itemwise, but its internal logic manages
        the parallel execution and merging of its branches.
        """

        @stage(name=f"Branch(merge='{self.merge}')", stage_type="itemwise")
        def _branch_func(context: "Context", item: Any) -> Iterable[Any]:
            # Create an iterator for each branch for the current item
            branch_iterators = [
                branch.run([item], collect=False)[0] for branch in self.branches
            ]

            # --- Merging Logic ---
            if self.merge == "concat":
                for it in branch_iterators:
                    yield from it

            elif self.merge == "zip":
                # Use zip for lazy, parallel iteration
                for zipped_items in zip(*branch_iterators):
                    yield zipped_items

            elif self.merge == "zip_longest":
                # Use zip_longest to continue until all branches are exhausted
                for zipped_items in zip_longest(*branch_iterators, fillvalue=None):
                    yield zipped_items

        return _branch_func
