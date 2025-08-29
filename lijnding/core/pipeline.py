from __future__ import annotations
from typing import Any, Iterable, List, Optional, Tuple, Union

from ..backends.runner_registry import get_runner
from .context import Context
from .stage import Stage, stage


class Pipeline:
    """
    A sequence of stages that process data.
    """

    def __init__(self, stages: Optional[List[Stage]] = None, *, name: Optional[str] = None):
        self.stages: List[Stage] = stages or []
        self.name = name or "Pipeline"

    def __or__(self, other: Any) -> "Pipeline":
        """
        Composes this pipeline with another component (Stage, Pipeline, or Branch).
        """
        # Local import to prevent circular dependency
        from ..components.branch import Branch

        other_stage: Stage
        if isinstance(other, Stage):
            other_stage = other
        elif isinstance(other, Pipeline):
            # If the other item is a full pipeline, convert it to a single stage
            other_stage = other.to_stage()
        elif isinstance(other, Branch):
            # If it's a branch, convert it to a stage
            other_stage = other.to_stage()
        else:
            raise TypeError(f"Unsupported type for pipeline composition: {type(other)}")

        # If this pipeline is empty, create a new one starting with the new stage.
        # Otherwise, add the new stage to the existing list.
        if self.stages and self.stages[-1].stage_type == "source" and other_stage.stage_type == "source":
            raise TypeError("Cannot pipe from one 'source' stage to another.")

        if not self.stages:
             return Pipeline([other_stage])
        return Pipeline(self.stages + [other_stage])


    def __rshift__(self, other: Union[Stage, "Pipeline"]) -> "Pipeline":
        return self.__or__(other)

    def _get_required_backend_names(self) -> set[str]:
        return set(getattr(s, "backend", "serial") for s in self.stages)

    def _build_context(self) -> Context:
        needs_mp = "process" in self._get_required_backend_names()
        return Context(mp_safe=needs_mp)

    def run(
        self, data: Iterable[Any], *, collect: bool = False
    ) -> Tuple[Union[List[Any], Iterable[Any]], Context]:
        context = self._build_context()
        stream: Iterable[Any] = data

        for stage in self.stages:
            runner = get_runner(getattr(stage, "backend", "serial"))
            stream = runner.run(stage, context, stream)

        if collect:
            return list(stream), context
        return stream, context

    def collect(self, data: Iterable[Any]) -> Tuple[List[Any], Context]:
        stream, context = self.run(data, collect=True)
        # The type hint says stream can be an iterable, but collect=True ensures it's a list
        return stream, context # type: ignore

    async def run_async(
        self, data: Union[Iterable[Any], AsyncIterable[Any]]
    ) -> Tuple[AsyncIterator[Any], Context]:
        """
        Asynchronously executes the pipeline.
        This method is required for pipelines that use the 'async' backend.
        """
        context = self._build_context()

        # Ensure the input is an async iterable
        async def _to_async(it):
            for i in it:
                yield i

        stream: AsyncIterable[Any]
        if not hasattr(data, "__aiter__"):
            stream = _to_async(data)
        else:
            stream = data # type: ignore

        for stage in self.stages:
            runner = get_runner(getattr(stage, "backend", "serial"))
            if hasattr(runner, 'run_async'):
                stream = runner.run_async(stage, context, stream)
            else:
                # This is a synchronous bridge for running a sync stage in an async pipeline
                # It's not perfectly non-blocking but is a pragmatic solution.
                # A more advanced implementation would run this in a thread pool.
                sync_results = runner.run(stage, context, [item async for item in stream])
                stream = _to_async(sync_results)

        return stream, context

    @property
    def metrics(self) -> dict[str, Any]:
        stage_metrics = {stage.name: stage.metrics for stage in self.stages}
        return {"stages": stage_metrics}

    def to_stage(self) -> Stage:
        """Converts the entire pipeline into a single, reusable Stage."""
        pipeline_name = self.name or "nested_pipeline"

        @stage(name=pipeline_name, stage_type="itemwise")
        def _pipeline_as_stage_func(context: Context, item: Any) -> Iterable[Any]:
            inner_results, _ = self.run(data=[item], collect=True)
            yield from inner_results

        return _pipeline_as_stage_func

    def __repr__(self) -> str:
        stage_names = " | ".join(s.name for s in self.stages)
        return f"Pipeline(name='{self.name}', stages=[{stage_names}])"
