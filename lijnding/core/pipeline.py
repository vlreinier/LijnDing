from __future__ import annotations
from typing import Any, Iterable, List, Optional, Tuple, Union, AsyncIterator, AsyncIterable
import time

from ..backends.runner_registry import get_runner
from .context import Context
from .errors import PipelineConnectionError
from .stage import Stage, stage
from .log import get_logger
from ..typing.checker import are_types_compatible


class Pipeline:
    """
    A sequence of stages that process data.
    """

    def __init__(self, stages: Optional[List[Stage]] = None, *, name: Optional[str] = None):
        self.stages: List[Stage] = stages or []
        self.name = name or "Pipeline"
        self.logger = get_logger(f"lijnding.pipeline.{self.name}")

    def add(self, other: Any) -> "Pipeline":
        """
        Adds a component (Stage or Pipeline) to this pipeline.
        This enables a fluent, chainable interface for building pipelines.
        """
        other_stage: Stage
        if isinstance(other, Stage):
            other_stage = other
        elif isinstance(other, Pipeline):
            other_stage = other.to_stage()
        else:
            raise TypeError(f"Unsupported type for pipeline composition: {type(other)}")

        if self.stages:
            last_stage = self.stages[-1]
            if last_stage.stage_type == "source" and other_stage.stage_type == "source":
                raise TypeError("Cannot pipe from one 'source' stage to another.")

            if last_stage.stage_type != "source":
                if not are_types_compatible(last_stage.output_type, other_stage.input_type):
                    raise PipelineConnectionError(
                        from_stage=last_stage,
                        to_stage=other_stage,
                        message="Type mismatch between stages.",
                    )
        self.stages.append(other_stage)
        return self

    def __or__(self, other: Any) -> "Pipeline":
        """
        Composes this pipeline with another component (Stage or Pipeline).
        """
        other_stage: Stage
        if isinstance(other, Stage):
            other_stage = other
        elif isinstance(other, Pipeline):
            # If the other item is a full pipeline, convert it to a single stage
            other_stage = other.to_stage()
        else:
            raise TypeError(f"Unsupported type for pipeline composition: {type(other)}")

        # If this pipeline is empty, create a new one starting with the new stage.
        # Otherwise, add the new stage to the existing list.
        if self.stages:
            last_stage = self.stages[-1]
            if last_stage.stage_type == "source" and other_stage.stage_type == "source":
                raise TypeError("Cannot pipe from one 'source' stage to another.")

            if last_stage.stage_type != "source":
                if not are_types_compatible(last_stage.output_type, other_stage.input_type):
                    raise PipelineConnectionError(
                        from_stage=last_stage,
                        to_stage=other_stage,
                        message="Type mismatch between stages.",
                    )

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
        self, data: Optional[Iterable[Any]] = None, *, collect: bool = False
    ) -> Tuple[Union[List[Any], Iterable[Any]], Context]:
        self.logger.info("Pipeline run started.")
        start_time = time.time()
        context = self._build_context()

        if data is None:
            if not self.stages or self.stages[0].stage_type != "source":
                raise TypeError("Pipeline.run() requires a data argument unless the first stage is a source stage.")
            data = []

        stream: Iterable[Any] = data

        try:
            for stage in self.stages:
                runner = get_runner(getattr(stage, "backend", "serial"))
                stream = runner.run(stage, context, stream)

            if collect:
                return list(stream), context
            return stream, context
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Pipeline run finished in {total_time:.4f} seconds.")

    def collect(self, data: Optional[Iterable[Any]] = None) -> Tuple[List[Any], Context]:
        stream, context = self.run(data, collect=True)
        # The type hint says stream can be an iterable, but collect=True ensures it's a list
        return stream, context # type: ignore

    async def run_async(
        self, data: Optional[Union[Iterable[Any], AsyncIterable[Any]]] = None
    ) -> Tuple[AsyncIterator[Any], Context]:
        """
        Asynchronously executes the pipeline.
        This method is required for pipelines that use the 'async' backend.
        """
        self.logger.info("Async pipeline run started.")
        start_time = time.time()
        context = self._build_context()

        if data is None:
            if not self.stages or self.stages[0].stage_type != "source":
                raise TypeError("Pipeline.run_async() requires a data argument unless the first stage is a source stage.")
            data = []

        # Ensure the input is an async iterable
        async def _to_async(it):
            for i in it:
                yield i

        stream: AsyncIterable[Any]
        if not hasattr(data, "__aiter__"):
            stream = _to_async(data)
        else:
            stream = data # type: ignore

        try:
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
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Async pipeline run finished in {total_time:.4f} seconds.")

    @property
    def metrics(self) -> dict[str, Any]:
        stage_metrics = {stage.name: stage.metrics for stage in self.stages}
        return {"stages": stage_metrics}

    def to_stage(self) -> Stage:
        """
        Converts the entire pipeline into a single, reusable Stage.
        If the pipeline contains any async stages, this will produce an async Stage.
        """
        pipeline_name = self.name or "nested_pipeline"

        is_async_pipeline = "async" in self._get_required_backend_names()

        if is_async_pipeline:
            @stage(name=pipeline_name, stage_type="itemwise", backend="async")
            async def _pipeline_as_stage_func_async(context: Context, item: Any) -> AsyncIterator[Any]:
                stream, _ = await self.run_async(data=[item])
                async for inner_item in stream:
                    yield inner_item
            return _pipeline_as_stage_func_async
        else:
            @stage(name=pipeline_name, stage_type="itemwise")
            def _pipeline_as_stage_func_sync(context: Context, item: Any) -> Iterable[Any]:
                inner_results, _ = self.run(data=[item], collect=True)
                yield from inner_results
            return _pipeline_as_stage_func_sync

    def __repr__(self) -> str:
        stage_names = " | ".join(s.name for s in self.stages)
        return f"Pipeline(name='{self.name}', stages=[{stage_names}])"
