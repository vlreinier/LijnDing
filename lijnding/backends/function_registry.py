from typing import Callable, Dict

# A simple registry for functions that can be used in multiprocessing
_function_registry: Dict[str, Callable] = {}

def register_function(func: Callable, name: str = None):
    """Registers a function for use in the ProcessingRunner."""
    key = name or f"{func.__module__}.{func.__name__}"
    if key in _function_registry:
        pass
    _function_registry[key] = func

def get_function(name: str) -> Callable:
    """Retrieves a function from the registry."""
    if name not in _function_registry:
        raise NameError(f"Function '{name}' is not registered for multiprocessing.")
    return _function_registry[name]
