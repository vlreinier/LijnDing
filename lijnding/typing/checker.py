from typing import Any, get_args, get_origin


def check_instance(value: Any, annotation: Any) -> bool:
    """
    Best-effort runtime type check that supports generics like List[int].
    Falls back to True if the annotation is Any or cannot be enforced.
    """
    if annotation is Any or annotation is None:
        return True

    origin = get_origin(annotation)
    args = get_args(annotation)

    # Plain class or typing alias without args
    if origin is None:
        try:
            return isinstance(value, annotation)
        except TypeError:
            # This can happen for complex types that aren't classes, like Union
            return True

    # Handle common containers from `typing`
    if origin in (list, tuple, set, frozenset):
        container_type = list if origin is list else tuple if origin is tuple else set
        if not isinstance(value, container_type):
            return False
        if not args:
            return True # e.g., list vs List[Any]

        item_type = args[0]
        if len(args) == 2 and args[1] is Ellipsis: # e.g., Tuple[int, ...]
            return all(check_instance(v, item_type) for v in value)

        return all(check_instance(v, item_type) for v in value)

    if origin is dict:
        if not isinstance(value, dict):
            return False
        if not args:
            return True
        key_type, value_type = args
        return all(check_instance(k, key_type) and check_instance(v, value_type) for k, v in value.items())

    # Fallback for types we don't handle explicitly
    return True
