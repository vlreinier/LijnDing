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
        return are_types_compatible(args_out[0], input_type) if args_out else True

    if origin_in is Union:
        return any(are_types_compatible(output_type, arg) for arg in args_in)
    if origin_out is Union:
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
