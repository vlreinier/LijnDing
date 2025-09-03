from __future__ import annotations
from typing import Any, Iterable, List, Optional, Tuple, Union, AsyncIterator, AsyncIterable
import time
import asyncio

from ..backends.runner_registry import get_runner
from .context import Context
from .errors import PipelineConnectionError
from .stage import Stage, stage
from .log import get_logger
from .utils import AsyncToSyncIterator
from ..config import Config, load_config
from typing import Iterable


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
                # If the stream is an async generator, we need to run it in an event loop
                # to collect the results.
                if hasattr(stream, "__aiter__"):
                    async def _collect_async(async_stream):
                        return [item async for item in async_stream]

                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                    return loop.run_until_complete(_collect_async(stream)), context
                else:
                    return list(stream), context
            return stream, context
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Pipeline run finished in {total_time:.4f} seconds.")

    def collect(self, data: Optional[Iterable[Any]] = None, config_path: Optional[str] = None) -> Tuple[List[Any], Context]:
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
                    # We use the AsyncToSyncIterator to bridge the async and sync worlds
                    # in a streaming, memory-efficient way.
                    loop = asyncio.get_running_loop()
                    sync_iterable = AsyncToSyncIterator(stream, loop)

                    # We still run the synchronous stage in a thread pool to avoid
                    # blocking the event loop while it processes.
                    def run_sync_stage_in_thread():
                        # This function will run in a separate thread.
                        # If the runner needs its own event loop, we create one here.
                        if runner.should_run_in_own_loop():
                            new_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(new_loop)
                            try:
                                return list(runner.run(stage, context, sync_iterable))
                            finally:
                                new_loop.close()
                        else:
                            # Otherwise, just run it directly.
                            return list(runner.run(stage, context, sync_iterable))

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
        Converts the entire pipeline into a single, reusable Stage.
        If the pipeline contains any async stages, this will produce an async Stage.
        """
        pipeline_name = self.name or "nested_pipeline"

        is_async_pipeline = "async" in self._get_required_backend_names()

        if is_async_pipeline:
            @stage(
                name=pipeline_name,
                stage_type="itemwise",
                backend="async",
                wrapped_pipeline=self,
            )
            async def _pipeline_as_stage_func_async(context: Context, item: Any) -> AsyncIterator[Any]:
                stream, _ = await self.run_async(data=[item])
                async for inner_item in stream:
                    yield inner_item
            return _pipeline_as_stage_func_async
        else:
            @stage(
                name=pipeline_name,
                stage_type="itemwise",
                wrapped_pipeline=self,
            )
            def _pipeline_as_stage_func_sync(context: Context, item: Any) -> Iterable[Any]:
                inner_results, _ = self.run(data=[item], collect=True)
                yield from inner_results
            return _pipeline_as_stage_func_sync

    def visualize(self) -> str:
        """
        Generates a DOT graph representation of the pipeline.

        The output can be rendered using Graphviz.
        Example: `dot -Tpng -o pipeline.png pipeline.dot`
        """
        import re
        import textwrap

        def get_node_id(obj: Any) -> str:
            """Creates a unique and DOT-compatible ID for an object."""
            return f'"{id(obj)}"'

        def escape_label(label: str) -> str:
            """Escapes a string for use as a DOT label."""
            return label.replace('"', '\\"').replace("{", "\\{").replace("}", "\\}")

        def format_label(name: str, stage: Optional[Stage] = None) -> str:
            """Creates a formatted label for a node."""
            name = escape_label(name)
            if not stage:
                return f'"{name}"'

            label_parts = [name]
            if stage.backend != "serial":
                label_parts.append(f"backend: {stage.backend}")
            if stage.workers > 1:
                label_parts.append(f"workers: {stage.workers}")

            return f'"{"\\n".join(label_parts)}"'


        def _traverse(pipeline: "Pipeline", graph_name: str, parent_node: Optional[str] = None) -> Tuple[List[str], Optional[str], Optional[str]]:
            """
            Recursively traverses the pipeline and generates DOT graph statements.
            Returns a tuple of (dot_statements, first_node_id, last_node_id).
            """
            dot: List[str] = []
            prefix = "  " * (graph_name.count("cluster") + 1)
            first_node = None
            last_node = parent_node

            if not pipeline.stages:
                return [], None, None

            # Create a subgraph for the current pipeline
            dot.append(f'{prefix}subgraph "{graph_name}" {{')
            dot.append(f'{prefix}  label = "{escape_label(pipeline.name)}";')
            dot.append(f'{prefix}  style = "rounded";')


            for i, stage in enumerate(pipeline.stages):
                node_id = get_node_id(stage)
                if not first_node:
                    first_node = node_id

                # Handling for branch stages
                if stage.branch_pipelines:
                    branch_cluster_name = f"cluster_branch_{id(stage)}"
                    dot.append(f'{prefix}  subgraph "{branch_cluster_name}" {{')
                    dot.append(f'{prefix}    label = "{escape_label(stage.name)}";')
                    dot.append(f'{prefix}    style = "dashed";')

                    branch_entry_id = f'"branch_entry_{id(stage)}"'
                    branch_exit_id = f'"branch_exit_{id(stage)}"'
                    dot.append(f'{prefix}    {branch_entry_id} [label="start", shape=circle, style=filled, fillcolor=grey];')
                    dot.append(f'{prefix}    {branch_exit_id} [label="end", shape=circle, style=filled, fillcolor=grey];')


                    for j, branch_pipeline in enumerate(stage.branch_pipelines):
                        branch_graph_name = f"cluster_branch_{id(stage)}_sub_{j}"
                        branch_dot, branch_first, branch_last = _traverse(branch_pipeline, branch_graph_name)
                        dot.extend(branch_dot)
                        if branch_first:
                            dot.append(f"{prefix}    {branch_entry_id} -> {branch_first};")
                        if branch_last:
                            dot.append(f"{prefix}    {branch_last} -> {branch_exit_id};")

                    dot.append(f'{prefix}  }}')

                    if last_node:
                        dot.append(f"{prefix}  {last_node} -> {branch_entry_id};")
                    last_node = branch_exit_id

                # Handling for nested pipelines
                elif stage.wrapped_pipeline:
                    nested_pipeline = stage.wrapped_pipeline
                    nested_cluster_name = f"cluster_nested_{id(stage)}"
                    nested_dot, nested_first, nested_last = _traverse(nested_pipeline, nested_cluster_name)
                    dot.extend(nested_dot)
                    if last_node and nested_first:
                        dot.append(f"{prefix}  {last_node} -> {nested_first};")
                    if nested_last:
                        last_node = nested_last

                # Handling for regular stages
                else:
                    label = format_label(stage.name, stage)
                    dot.append(f"{prefix}  {node_id} [label={label}, shape=box];")
                    if last_node:
                        dot.append(f"{prefix}  {last_node} -> {node_id};")
                    last_node = node_id


            dot.append(f"{prefix}}}")
            return dot, first_node, last_node


        # Main part of visualize()
        dot_lines = ["digraph G {", "  rankdir=TB;"]
        traversed_statements, _, _ = _traverse(self, "cluster_main", None)
        dot_lines.extend(traversed_statements)
        dot_lines.append("}")

        return "\n".join(dot_lines)


    def __repr__(self) -> str:
        stage_names = " | ".join(s.name for s in self.stages)
        return f"Pipeline(name='{self.name}', stages=[{stage_names}])"
