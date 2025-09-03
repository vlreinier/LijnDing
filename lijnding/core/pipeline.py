from __future__ import annotations
from typing import Any, Iterable, List, Optional, Tuple, Union, AsyncIterator, AsyncIterable
import time
import asyncio

from ..backends.runner_registry import get_runner
from .context import Context
from .errors import PipelineConnectionError
from .stage import Stage, stage
from .log import get_logger
from ..config import Config, load_config
from typing import Iterable


class Pipeline:
    """
    Represents a sequence of stages that process data.

    Pipelines are the main entry point for running data processing workflows.
    They are created by composing `Stage` objects using the `|` operator.

    Example:
        >>> pipeline = read_from_file("data.txt") | process_text | write_to_db
        >>> results, context = pipeline.collect()
    """

    def __init__(self, stages: Optional[List[Stage]] = None, *, name: Optional[str] = None):
        """
        Initializes a Pipeline.

        Args:
            stages (Optional[List[Stage]]): A list of stages to initialize
                the pipeline with. Defaults to an empty list.
            name (Optional[str]): A name for the pipeline, used for logging.
                Defaults to "Pipeline".
        """
        self.stages: List[Stage] = stages or []
        self.name = name or "Pipeline"
        self.logger = get_logger(f"lijnding.pipeline.{self.name}")

    def add(self, other: Any) -> "Pipeline":
        """
        Adds a `Stage` or `Pipeline` to the end of this pipeline.

        This method is used internally by the `|` operator but can be called
        manually. It modifies the pipeline in-place and returns it to allow
        for a fluent, chainable interface.

        Args:
            other: The `Stage` or `Pipeline` to add.

        Returns:
            The modified `Pipeline` instance.
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

        self.stages.append(other_stage)
        return self

    def __or__(self, other: Any) -> "Pipeline":
        """
        Composes this pipeline with another `Stage` or `Pipeline` using the `|` operator.

        This method creates a *new* `Pipeline` containing the stages of both
        operands. It does not modify the original pipeline.

        Args:
            other: The `Stage` or `Pipeline` to compose with.

        Returns:
            A new `Pipeline` instance.
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


        if not self.stages:
             return Pipeline([other_stage])
        return Pipeline(self.stages + [other_stage])


    def __rshift__(self, other: Union[Stage, "Pipeline"]) -> "Pipeline":
        return self.__or__(other)

    def _get_required_backend_names(self) -> set[str]:
        return set(getattr(s, "backend", "serial") for s in self.stages)

    def _build_context(self, config: Optional[Config] = None) -> Context:
        needs_mp = "process" in self._get_required_backend_names()
        return Context(mp_safe=needs_mp, config=config, pipeline_name=self.name)

    def run(
        self, data: Optional[Iterable[Any]] = None, *, collect: bool = False, config_path: Optional[str] = None
    ) -> Tuple[Union[List[Any], Iterable[Any]], Context]:
        """
        Executes the pipeline.

        This method runs the input data through all stages of the pipeline.

        Args:
            data (Optional[Iterable[Any]]): An iterable of data to process.
                If the first stage is a 'source' stage, this can be omitted.
            collect (bool): If `True`, returns the results as a list.
                Otherwise, returns an iterator. Defaults to `False`.
            config_path (Optional[str]): Path to a YAML configuration file
                to be loaded into the context.

        Returns:
            A tuple containing the results (either a list or an iterator)
            and the final `Context` object.
        """
        self.logger.info("Pipeline run started.")
        start_time = time.time()
        config = load_config(config_path)
        context = self._build_context(config)

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

    def collect(self, data: Optional[Iterable[Any]] = None, config_path: Optional[str] = None) -> Tuple[List[Any], Context]:
        """
        Executes the pipeline and collects all results into a list.

        This is a convenience method that is equivalent to calling `run()`
        with `collect=True`.

        Args:
            data (Optional[Iterable[Any]]): An iterable of data to process.
            config_path (Optional[str]): Path to a YAML configuration file.

        Returns:
            A tuple containing a list of results and the final `Context`.
        """
        stream, context = self.run(data, collect=True, config_path=config_path)
        # The type hint says stream can be an iterable, but collect=True ensures it's a list
        return stream, context # type: ignore

    async def run_async(
        self, data: Optional[Union[Iterable[Any], AsyncIterable[Any]]] = None, config_path: Optional[str] = None
    ) -> Tuple[AsyncIterator[Any], Context]:
        """
        Asynchronously executes the pipeline.
        This method is required for pipelines that use the 'async' backend.
        """
        self.logger.info("Async pipeline run started.")
        start_time = time.time()
        config = load_config(config_path)
        context = self._build_context(config)

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
                    # This is a synchronous bridge for running a sync stage in an async pipeline.
                    # We run the synchronous stage in a thread pool to avoid blocking the event loop.
                    items = [item async for item in stream]
                    loop = asyncio.get_running_loop()

                    def run_sync_stage_in_thread():
                        return list(runner.run(stage, context, items))

                    sync_results = await loop.run_in_executor(None, run_sync_stage_in_thread)
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
        Converts the entire pipeline into a single, reusable `Stage`.

        This is useful for nesting a complex pipeline within another one,
        treating the entire inner pipeline as a single processing step. The
        resulting stage is an 'itemwise' stage that will run each item through
        the inner pipeline.

        If the pipeline contains any `async` stages, this will produce an
        `async` stage that must be used in an async-compatible pipeline.

        Returns:
            A new `Stage` that encapsulates the logic of this pipeline.
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
