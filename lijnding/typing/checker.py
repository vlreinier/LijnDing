from typing import Any, Type, Union, get_origin, get_args, Iterable


def check_instance(obj: Any, type_hint: Type[Any]) -> bool:
    """
    A more robust version of `isinstance` that handles generic types from `typing`.
    """
    if type_hint is Any:
        return True

    origin = get_origin(type_hint)
    if origin is None:  # It's a non-generic type
        return isinstance(obj, type_hint)

    args = get_args(type_hint)
    if not args:  # e.g., `list` or `dict` without parameters
        return isinstance(obj, origin)

    if origin is Union:
        return any(check_instance(obj, arg) for arg in args)

    if not isinstance(obj, origin):
        return False

    # Handle specific generic types
    if origin in (list, set, frozenset) and len(args) == 1:
        return all(check_instance(item, args[0]) for item in obj)
    if origin is dict and len(args) == 2:
        return all(check_instance(k, args[0]) and check_instance(v, args[1]) for k, v in obj.items())
    if origin is tuple and len(args) > 0:
        if ... in args:  # Ellipsis, e.g., `Tuple[int, ...]`
            return all(check_instance(item, args[0]) for item in obj)
        return len(obj) == len(args) and all(check_instance(item, arg) for item, arg in zip(obj, args))

    return True  # Fallback for other generics


from typing import Any, Type, Union, get_origin, get_args, Iterable, Generator

def are_types_compatible(output_type: Type[Any], input_type: Type[Any]) -> bool:
    """
    Checks if the output type of one stage is compatible with the input type of another.
    This function handles generic types like `Any`, `Union`, and `Iterable`.
    """
    if input_type is Any or output_type is Any:
        return True

    origin_out = get_origin(output_type)
    origin_in = get_origin(input_type)
    args_out = get_args(output_type)
    args_in = get_args(input_type)

    if origin_out is Generator:
        output_type = args_out[0]
        origin_out = get_origin(output_type)
        args_out = get_args(output_type)

    if origin_in is Union:
        return any(are_types_compatible(output_type, arg) for arg in args_in)
    if origin_out is Union:
        # This is the incorrect logic. It should be any, not all.
        return any(are_types_compatible(arg, input_type) for arg in args_out)

    if origin_in is Iterable and origin_out is Iterable:
        if args_in and args_out:
            return are_types_compatible(args_out[0], args_in[0])
        return True

    try:
        return issubclass(output_type, input_type)
    except TypeError:
        if origin_out and origin_in:
            return issubclass(origin_out, origin_in)
        return output_type == input_type
