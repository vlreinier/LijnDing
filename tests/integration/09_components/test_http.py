import pytest
from aiohttp import web

from lijnding import Pipeline
from lijnding.components import http_request

@pytest.fixture
async def test_server(aiohttp_server):
    """A pytest fixture to create a simple test server."""
    async def handler(request):
        name = request.match_info.get('name', 'anonymous')
        return web.Response(text=f"Hello, {name}")

    app = web.Application()
    app.router.add_get('/greet/{name}', handler)
    return await aiohttp_server(app)

@pytest.mark.asyncio
async def test_http_request_component(test_server):
    """
    Tests the http_request component against a live test server.
    """
    server_url = f"http://{test_server.host}:{test_server.port}"
    data = ["jules", "ada", "grace"]

    # A pipeline that takes a name, builds a URL, and makes a request
    pipeline: Pipeline = http_request(
        url_builder=lambda name: f"{server_url}/greet/{name}"
    )

    # We need to use run_async and collect the results
    stream, _ = await pipeline.run_async(data)
    results = [item async for item in stream]

    expected = ["Hello, jules", "Hello, ada", "Hello, grace"]
    assert results == expected

@pytest.mark.asyncio
async def test_http_request_error_handling(test_server):
    """
    Tests that the http_request component correctly raises an error for a 404.
    """
    server_url = f"http://{test_server.host}:{test_server.port}"

    pipeline = http_request(lambda _: f"{server_url}/not_found")

    from aiohttp import ClientResponseError
    with pytest.raises(ClientResponseError) as excinfo:
        stream, _ = await pipeline.run_async(["item1"])
        _ = [item async for item in stream] # Consume the stream to trigger the error

    assert excinfo.value.status == 404
