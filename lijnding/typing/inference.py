import inspect
from typing import Any, Callable, Tuple, Type, get_type_hints


def infer_types(func: Callable[..., Any]) -> Tuple[Type[Any], Type[Any]]:
    """
    Infers input and output types from a function's type hints.

    It selects the first non-`context` parameter as the input type, and the
    return annotation as the output type.

    Returns:
        A tuple of (input_type, output_type).
    """
    try:
        hints = get_type_hints(func)
    except (TypeError, NameError):
        # This can happen with forward references that can't be resolved.
        # Fallback to Any.
        return Any, Any

    sig = inspect.signature(func)
    input_type: Any = Any

    for name, param in sig.parameters.items():
        if name == "context":
            continue
        # Find the first positional or keyword parameter to be the input
        if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
            input_type = hints.get(name, Any)
            break

    output_type: Any = hints.get("return", Any)

    return input_type, output_type
