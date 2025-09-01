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
        self.error_policy = error_policy or ErrorPolicy()
        self.hooks = hooks or Hooks()
        self.is_async = inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)

        inferred_in, inferred_out, num_args = infer_types(func)
        self.input_type = input_type or inferred_in
        self.output_type = output_type or inferred_out
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
) -> Union[Stage, Callable[[Callable[..., Any]], Stage]]:

    def wrapper(func: Callable[..., Any]) -> Stage:
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

    if _func is not None:
        return wrapper(_func)
    return wrapper
