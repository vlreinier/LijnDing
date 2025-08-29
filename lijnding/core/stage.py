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
)

from ..typing.inference import infer_types
from .context import Context
from .errors import ErrorPolicy
from .hooks import Hooks


# --- Utilities ---

def _ensure_iterable(x: Any) -> Iterable[Any]:
    """Ensures the input is an iterable, wrapping it in a list if necessary."""
    if x is None:
        return []
    if hasattr(x, "__iter__") and not isinstance(x, (str, bytes, dict)):
        return x
    return [x]


class Stage:
    """
    A Stage represents a single unit of work in a pipeline.

    It wraps a user-provided function and enriches it with features like
    error handling, type checking, context management, and backend execution.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: Optional[str] = None,
        stage_type: str = "itemwise",  # 'itemwise' | 'aggregator'
        backend: str = "serial",
        workers: int = 1,
        input_type: Optional[Type[Any]] = None,
        output_type: Optional[Type[Any]] = None,
        error_policy: Optional[ErrorPolicy] = None,
        hooks: Optional[Hooks] = None,
    ):
        self.func = func
        self.name = name or getattr(func, "__name__", "Stage")
        self.stage_type = stage_type
        self.backend = backend
        self.workers = workers
        self.error_policy = error_policy or ErrorPolicy()
        self.hooks = hooks or Hooks()

        # Type inference and context injection detection
        inferred_in, inferred_out = infer_types(func)
        self.input_type = input_type or inferred_in
        self.output_type = output_type or inferred_out
        self._inject_context = "context" in inspect.signature(func).parameters

        # Metrics
        self.metrics: dict[str, Any] = {
            "items_in": 0,
            "items_out": 0,
            "errors": 0,
            "time_total": 0.0,
        }

    def __repr__(self) -> str:
        return f"Stage(name='{self.name}', type='{self.stage_type}')"

    def __or__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        from .pipeline import Pipeline
        return Pipeline([self]) | other

    def __rshift__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        from .pipeline import Pipeline
        return Pipeline([self]) | other

    def _invoke(self, context: Context, *args: Any, **kwargs: Any) -> Any:
        """Invokes the stage's function, injecting context if required."""
        if self._inject_context:
            return self.func(context, *args, **kwargs)
        return self.func(*args, **kwargs)


def stage(
    _func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    stage_type: str = "itemwise",
    backend: str = "serial",
    workers: int = 1,
    input_type: Optional[Type[Any]] = None,
    output_type: Optional[Type[Any]] = None,
    error_policy: Optional[ErrorPolicy] = None,
    hooks: Optional[Hooks] = None,
    register_for_processing: bool = False,
) -> Union[Stage, Callable[[Callable[..., Any]], Stage]]:
    """
    A decorator to transform a function into a pipeline Stage.

    This is the primary way to create stages. It configures the stage's
    behavior, such as its name, error handling, and type.

    Args:
        name: A custom name for the stage. Defaults to the function name.
        stage_type: The type of stage.
            - 'itemwise': (Default) Processes one item at a time.
            - 'aggregator': Processes an entire iterable of items at once.
        backend: The execution backend ('serial', 'thread', 'process', 'async').
        workers: The number of workers for concurrent backends.
        input_type: Manually specify the expected input type.
        output_type: Manually specify the expected output type.
        error_policy: An ErrorPolicy object to configure error handling.
        hooks: A Hooks object to attach monitoring functions.
        register_for_processing: If True, registers the function so it can be
                                 used with the 'process' backend.
    """
    from ..backends.function_registry import register_function

    def wrapper(func: Callable[..., Any]) -> Stage:
        if register_for_processing:
            register_function(func, name=name)

        return Stage(
            func,
            name=name,
            stage_type=stage_type,
            backend=backend,
            workers=workers,
            input_type=input_type,
            output_type=output_type,
            error_policy=error_policy,
            hooks=hooks,
        )

    # This allows using @stage or @stage(...)
    if _func is not None:
        return wrapper(_func)
    return wrapper
