import asyncio
from typing import Callable, Any, Coroutine, AsyncGenerator

try:
    import aiohttp
except ImportError:
    raise ImportError(
        "The 'http_request' component requires the 'aiohttp' library. "
        "Please install it with: pip install lijnding[http]"
    )

from ..core.stage import stage, Stage


def http_request(
    url_builder: Callable[[Any], str],
    *,
    name: str = "http_request",
    method: str = "GET",
    headers: dict = None,
    # TODO: Add more options like body, timeout, etc.
) -> Stage:
    """
    Creates an async stage that performs an HTTP request for each incoming item.

    This component is designed to be used with an async backend (e.g., `asyncio`).
    It uses `aiohttp` to perform non-blocking HTTP requests.

    :param url_builder: A function that takes an item and returns the URL to request.
    :param name: An optional name for the stage.
    :param method: The HTTP method to use (e.g., 'GET', 'POST').
    :param headers: A dictionary of headers to include in the request.
    :return: A new asynchronous `Stage`.
    """
    @stage(name=name, backend="async", stage_type="itemwise")
    async def _http_request_stage(item: Any) -> AsyncGenerator[str, None]:
        """The actual async stage function that performs the HTTP request."""
        url = url_builder(item)
        async with aiohttp.ClientSession(headers=headers) as session:
            try:
                async with session.request(method, url) as response:
                    response.raise_for_status()
                    # Yield the response text. Could be extended to yield response object.
                    yield await response.text()
            except aiohttp.ClientError as e:
                # Here, we could log the error and decide whether to raise or skip.
                # For now, we re-raise to let the pipeline's error handling take over.
                # In a real-world scenario, you might want more granular control.
                # For example: `stage.logger.error(...)`
                raise e

    return _http_request_stage
