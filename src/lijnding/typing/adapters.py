from typing import Any, Callable, Dict, Tuple


class TypeAdapters:
    """
    A registry for functions that can coerce a value from one type to another.
    """

    def __init__(self):
        self._adapters: Dict[Tuple[Any, Any], Callable[[Any], Any]] = {}

    def register(self, src: Any, dst: Any, func: Callable[[Any], Any]) -> None:
        """Register a new type adapter function."""
        self._adapters[(src, dst)] = func

    def adapt(self, value: Any, dst: Any) -> Tuple[bool, Any]:
        """
        Tries to adapt a value to the destination type.

        Returns a tuple of (success, new_value).
        """
        # Try exact match first
        key = (type(value), dst)
        if key in self._adapters:
            try:
                return True, self._adapters[key](value)
            except Exception:
                return False, value

        # Try common simple casts as a fallback
        try:
            if dst in (str, int, float, bool):
                return True, dst(value)
        except (TypeError, ValueError):
            pass

        return False, value


# A default instance for convenience
DEFAULT_ADAPTERS = TypeAdapters()
