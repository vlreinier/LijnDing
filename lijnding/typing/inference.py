import inspect
from typing import Any, Callable, Tuple, Type, get_type_hints


def infer_types(func: Callable[..., Any]) -> Tuple[Type[Any], Type[Any], int]:
    """
    Infers types from a function's hints and counts input arguments.

    Returns:
        A tuple of (input_type, output_type, non_context_arg_count).
    """
    try:
        hints = get_type_hints(func)
    except (TypeError, NameError):
        return Any, Any, 1 # Assume 1 argument on failure

    sig = inspect.signature(func)
    input_type: Any = Any
    arg_count = 0

    # Find the first positional-or-keyword parameter to be the input type
    found_input = False
    for name, param in sig.parameters.items():
        if name == "context":
            continue
        arg_count += 1
        if not found_input and param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
            input_type = hints.get(name, Any)
            found_input = True

    output_type: Any = hints.get("return", Any)

    return input_type, output_type, arg_count
