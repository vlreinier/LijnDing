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


def _ensure_iterable(x: Any) -> Iterable[Any]:
    if x is None:
        return []
    if hasattr(x, "__iter__") and not isinstance(x, (str, bytes, dict)):
        return x
    return [x]


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
        self.stage_type = stage_type
        self.backend = backend
        self.workers = workers
        self.error_policy = error_policy or ErrorPolicy()
        self.hooks = hooks or Hooks()

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
        if self.stage_type == "source":
            return self.func(context) if self._inject_context else self.func()

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

    if _func is not None:
        return wrapper(_func)
    return wrapper
