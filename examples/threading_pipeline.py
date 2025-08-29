"""
An example demonstrating the use of the 'thread' backend for concurrent I/O-bound tasks.
"""
import time
from lijnding import Pipeline, stage

# A list of dummy URLs to "download"
URLS = [
    "http://example.com/page1",
    "http://example.com/page2",
    "http://example.com/page3",
    "http://example.com/page4",
    "http://example.com/page5",
    "http://example.com/page6",
]

# Use the @stage decorator and specify the backend and number of workers.
@stage(backend="thread", workers=4)
def download_url(url: str):
    """
    A dummy function to simulate downloading a URL.
    It sleeps for 1 second to represent network latency.
    """
    print(f"Downloading {url}...")
    time.sleep(1)
    print(f"Finished {url}.")
    return {"url": url, "content": f"Content of {url}"}

def main():
    """Builds and runs the concurrent pipeline."""
    print("--- Starting pipeline with 'thread' backend ---")

    # Construct the pipeline
    pipeline = Pipeline() | download_url

    # Run the pipeline and time it
    start_time = time.perf_counter()
    results, _ = pipeline.collect(URLS)
    end_time = time.perf_counter()

    print("\n--- Results ---")
    print(f"Downloaded {len(results)} pages.")

    total_time = end_time - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds.")
    print("Since we have 4 workers and 6 URLs, this should be much faster than 6 seconds.")

if __name__ == "__main__":
    main()
