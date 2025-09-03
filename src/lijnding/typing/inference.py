import inspect
from typing import Any, Callable, Tuple, Type, get_type_hints


def infer_types(func: Callable[..., Any]) -> Tuple[Type[Any], Type[Any], int]:
    """
    Infers types from a function's hints and counts input arguments.
    If hints are not present, it defaults to `Any`.

    Returns:
        A tuple of (input_type, output_type, non_context_arg_count).
    """
    hints = {}
    try:
        # We use include_extras=True to handle Annotated types if they are used.
        hints = get_type_hints(func, include_extras=True)
    except (TypeError, NameError):
        # This can happen with forward references that can't be resolved,
        # or with some callables (e.g. built-ins) that don't support type hints.
        pass  # Fallback to Any below

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        # If we can't get a signature, we can't know the arg count for sure.
        # Assume 1 argument if signature fails.
        return Any, Any, 1

    input_type: Any = None
    arg_count = 0

    # Find the first positional-or-keyword parameter to be the input type
    for name, param in sig.parameters.items():
        if name == "context":
            continue
        arg_count += 1
        if input_type is None and param.kind in (
            param.POSITIONAL_ONLY,
            param.POSITIONAL_OR_KEYWORD,
        ):
            input_type = hints.get(name)

    # Get the return type hint
    output_type = hints.get("return")

    # If the direct hint is not found, check the signature's return annotation.
    if output_type is None and sig.return_annotation is not inspect.Signature.empty:
        output_type = sig.return_annotation

    return input_type or Any, output_type or Any, arg_count
