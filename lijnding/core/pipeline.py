from __future__ import annotations

from typing import Any, Iterable, List, Optional, Tuple, Union

from ..backends.registry import get_runner
from .context import Context
from .stage import Stage


class Pipeline:
    """
    A sequence of stages that process data.

    The Pipeline class is the main entry point for constructing and executing
    data processing workflows. It chains together stages and manages the flow
    of data between them.
    """

    def __init__(self, stages: Optional[List[Stage]] = None, *, name: Optional[str] = None):
        self.stages: List[Stage] = stages or []
        self.name = name or "Pipeline"
        self._metrics: Dict[str, Any] = {"stages": {}}

    def __or__(self, other: Union[Stage, "Pipeline"]) -> "Pipeline":
        """
        Composes this pipeline with another stage or pipeline using the `|` operator.
        """
        if isinstance(other, Pipeline):
            return Pipeline(self.stages + other.stages)
        if isinstance(other, Stage):
            return Pipeline(self.stages + [other])
        raise TypeError(f"Unsupported type for pipeline composition: {type(other)}")

    def __rshift__(self, other: Union[Stage, "Pipeline"]) -> "Pipeline":
        """

        Composes this pipeline with another stage or pipeline using the `>>` operator.
        """
        return self.__or__(other)

    def _get_required_backend_names(self) -> set[str]:
        """Inspects stages to see which backends are needed."""
        # This is a placeholder for now. The full implementation will depend on the backend refactoring.
        return set(getattr(s, "backend", "serial") for s in self.stages)

    def _build_context(self) -> Context:
        """Creates a context, making it process-safe if needed."""
        # This is a placeholder.
        needs_mp = "process" in self._get_required_backend_names()
        return Context(mp_safe=needs_mp)

    def run(
        self, data: Iterable[Any], *, collect: bool = False
    ) -> Tuple[Union[List[Any], Iterable[Any]], Context]:
        """
        Executes the pipeline with the given input data.

        By default, execution is lazy, returning a generator.

        Args:
            data: The initial iterable of data to feed into the pipeline.
            collect: If True, consumes the entire generator and returns a list.

        Returns:
            A tuple containing the result (list or iterable) and the final Context.
        """
        context = self._build_context()
        stream: Iterable[Any] = data

        for stage in self.stages:
            # This is where the magic happens. We get a runner based on the
            # stage's configured backend (e.g., 'serial', 'thread').
            # The runner will handle the execution logic.
            # NOTE: Backends are not implemented yet in this step. This is a placeholder.
            # For now, we'll simulate a simple serial runner.

            runner = get_runner(getattr(stage, "backend", "serial"))
            stream = runner.run(stage, context, stream)

        if collect:
            return list(stream), context
        return stream, context

    def collect(self, data: Iterable[Any]) -> Tuple[List[Any], Context]:
        """
        Executes the pipeline and collects all results into a list.
        """
        stream, context = self.run(data, collect=True)
        return stream, context

    @property
    def metrics(self) -> dict[str, Any]:
        """Returns the collected metrics from all stages in the pipeline."""
        # This will be properly implemented once metrics collection is refactored.
        stage_metrics = {stage.name: stage.metrics for stage in self.stages}
        return {"stages": stage_metrics}

    def __repr__(self) -> str:
        stage_names = " | ".join(s.name for s in self.stages)
        return f"Pipeline(name='{self.name}', stages=[{stage_names}])"
