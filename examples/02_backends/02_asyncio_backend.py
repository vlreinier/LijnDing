"""
An example demonstrating the use of the 'async' backend for non-blocking
I/O-bound tasks using asyncio.
"""
import asyncio
from lijnding import stage

# A list of dummy URLs to "download"
URLS = [
    "http://example.com/page1",
    "http://example.com/page2",
    "http://example.com/page3",
]

# Use the @stage decorator and specify the backend.
# The stage function must be an `async def` function.
@stage(backend="async")
async def download_url_async(url: str):
    """
    A dummy function to simulate downloading a URL asynchronously.
    It uses `asyncio.sleep` to represent non-blocking network latency.
    """
    print(f"Starting download for {url}...")
    await asyncio.sleep(1) # This is non-blocking
    print(f"Finished {url}.")
    return {"url": url, "content": f"Content of {url}"}

async def main():
    """Builds and runs the async pipeline."""
    print("--- Starting pipeline with 'async' backend ---")

    # Construct the pipeline
    pipeline = download_url_async

    # To run an async pipeline, you must use the `run_async` method
    # and `await` it.
    stream, _ = await pipeline.run_async(URLS)

    # You can then iterate over the results using `async for`.
    results = [item async for item in stream]

    print("\n--- Results ---")
    print(f"Downloaded {len(results)} pages.")

if __name__ == "__main__":
    # To run the `main` async function, you use `asyncio.run()`
    asyncio.run(main())
