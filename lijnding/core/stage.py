from __future__ import annotations

import inspect
from functools import wraps
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    Type,
    Union,
    List,
    Tuple,
    AsyncIterator,
    AsyncIterable,
)

from ..typing.inference import infer_types
from .context import Context
from .errors import ErrorPolicy
from .hooks import Hooks
from .log import get_logger


class Stage:
    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: Optional[str] = None,
        stage_type: str = "itemwise",
        backend: str = "serial",
        workers: int = 1,
        buffer_size: Optional[int] = None,
        input_type: Optional[Type[Any]] = None,
        output_type: Optional[Type[Any]] = None,
        error_policy: Optional[ErrorPolicy] = None,
        hooks: Optional[Hooks] = None,
    ):
        self.func = func
        self.name = name or getattr(func, "__name__", "Stage")
        self.logger = get_logger(f"lijnding.stage.{self.name}")
        self.stage_type = stage_type
        self.backend = backend
        self.workers = workers
        self.buffer_size = buffer_size
        self.error_policy = error_policy or ErrorPolicy()
        self.hooks = hooks or Hooks()
        self.is_async = inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)

        self._input_type_override = input_type
        self._output_type_override = output_type

        inferred_in, inferred_out, num_args = infer_types(func)
        self.input_type = self._input_type_override or inferred_in
        self.output_type = self._output_type_override or inferred_out
        self._inject_context = "context" in inspect.signature(func).parameters

        if num_args == 0:
            self.stage_type = "source"

        self.metrics: dict[str, Any] = {
            "items_in": 0, "items_out": 0, "errors": 0, "time_total": 0.0,
        }

    def __repr__(self) -> str:
        return f"Stage(name='{self.name}', type='{self.stage_type}')"

    def __or__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        from .pipeline import Pipeline
        return Pipeline([self]) | other

    def __rshift__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        from .pipeline import Pipeline
        return Pipeline([self]) | other

    def run(
        self, data: Optional[Iterable[Any]] = None, *, collect: bool = False
    ) -> Tuple[Union[List[Any], Iterable[Any]], Context]:
        """
        Executes the stage as a single-stage pipeline.

        :param data: An iterable of data to process. If the stage is a source,
                     this can be omitted.
        :param collect: If True, returns the results as a list. Otherwise, returns an iterator.
        :return: A tuple containing the results and the execution context.
        """
        from .pipeline import Pipeline
        pipeline = Pipeline([self])
        return pipeline.run(data, collect=collect)

    def collect(self, data: Optional[Iterable[Any]] = None) -> Tuple[List[Any], Context]:
        """
        Executes the stage and collects all results into a list.

        :param data: An iterable of data to process. If the stage is a source,
                     this can be omitted.
        :return: A tuple containing the list of results and the execution context.
        """
        from .pipeline import Pipeline
        pipeline = Pipeline([self])
        return pipeline.collect(data)

    async def run_async(
        self, data: Optional[Union[Iterable[Any], AsyncIterable[Any]]] = None
    ) -> Tuple[AsyncIterator[Any], Context]:
        """
        Asynchronously executes the stage as a single-stage pipeline.

        :param data: An iterable or async iterable of data to process.
                     If the stage is a source, this can be omitted.
        :return: A tuple containing an async iterator for the results and the execution context.
        """
        from .pipeline import Pipeline
        pipeline = Pipeline([self])
        return await pipeline.run_async(data)

    def _invoke(self, context: Context, *args: Any, **kwargs: Any) -> Any:
        if self._inject_context:
            # Temporarily attach the stage-specific logger to the context
            # for the duration of this call.
            original_logger = context.logger
            context.logger = self.logger
            try:
                if self.stage_type == "source":
                    return self.func(context)
                return self.func(context, *args, **kwargs)
            finally:
                # Restore the original logger to avoid side effects
                context.logger = original_logger
        else:
            # Context is not injected, so just call the function
            if self.stage_type == "source":
                return self.func()
            return self.func(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """
        Provides a more helpful error message if a user tries to call a
        Pipeline-specific method on a Stage.
        """
        from .pipeline import Pipeline

        # Check if the attribute exists on the Pipeline class
        if hasattr(Pipeline, name):
            # Exclude methods that are intentionally on Stage
            if name in ("run", "run_async", "collect"):
                # This should not be reached if methods are defined, but as a safeguard.
                raise AttributeError(f"'Stage' object has no attribute '{name}'")

            message = (
                f"'Stage' object has no attribute '{name}'. "
                f"Did you mean to wrap it in a Pipeline first? "
                f"e.g., Pipeline([{self.name}]).{name}(...)"
            )
            raise AttributeError(message)

        # If the attribute is not on Pipeline, raise the default error.
        raise AttributeError(f"'Stage' object has no attribute '{name}'")


def stage(
    _func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    stage_type: str = "itemwise",
    backend: str = "serial",
    workers: int = 1,
    buffer_size: Optional[int] = None,
    input_type: Optional[Type[Any]] = None,
    output_type: Optional[Type[Any]] = None,
    error_policy: Optional[ErrorPolicy] = None,
    hooks: Optional[Hooks] = None,
) -> Union[Stage, Callable[[Callable[..., Any]], Stage]]:
    """
    A decorator to create a pipeline Stage from a function.

    This is the primary way to define the building blocks of a pipeline.

    Args:
        name (Optional[str]): A custom name for the stage. If not provided,
            the function's name is used.
        stage_type (str): The type of stage. Can be 'itemwise', 'aggregator',
            or 'source'. Defaults to 'itemwise'.
        backend (str): The execution backend to use for this stage.
            Defaults to 'serial'.
        workers (int): The number of parallel workers to use for concurrent
            backends ('thread', 'process'). Defaults to 1.
        buffer_size (Optional[int]): The maximum number of items to buffer in
            the input queue for concurrent backends. If not provided, a
            default value (typically `workers * 2`) is used.
        input_type (Optional[Type[Any]]): The expected input type for this
            stage. Used for static analysis.
        output_type (Optional[Type[Any]]): The expected output type for this
            stage. Used for static analysis.
        error_policy (Optional[ErrorPolicy]): The error handling policy for
            this stage.
        hooks (Optional[Hooks]): A collection of hooks for monitoring and
            tracing.

    Returns:
        A Stage object or a decorator that returns a Stage object.
    """
    def wrapper(func: Callable[..., Any]) -> Stage:
        return Stage(
            func,
            name=name,
            stage_type=stage_type,
            backend=backend,
            workers=workers,
            buffer_size=buffer_size,
            input_type=input_type,
            output_type=output_type,
            error_policy=error_policy,
            hooks=hooks,
        )

    if _func is not None:
        return wrapper(_func)
    return wrapper


def aggregator_stage(
    _func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    backend: str = "serial",
    workers: int = 1,
    buffer_size: Optional[int] = None,
    input_type: Optional[Type[Any]] = None,
    output_type: Optional[Type[Any]] = None,
    error_policy: Optional[ErrorPolicy] = None,
    hooks: Optional[Hooks] = None,
) -> Union[Stage, Callable[[Callable[..., Any]], Stage]]:
    """
    A decorator to create an aggregator stage.

    This is a convenience decorator that is equivalent to using `@stage`
    with `stage_type="aggregator"`. Aggregator stages receive the entire
    input stream as a single iterable argument.

    All arguments from the `@stage` decorator are also accepted here.
    """
    # We ignore the type checking error here because we are intentionally
    # passing the `_func` argument to the `stage` decorator, which knows
    # how to handle it.
    return stage(
        _func,  # type: ignore
        name=name,
        stage_type="aggregator",
        backend=backend,
        workers=workers,
        buffer_size=buffer_size,
        input_type=input_type,
        output_type=output_type,
        error_policy=error_policy,
        hooks=hooks,
    )
