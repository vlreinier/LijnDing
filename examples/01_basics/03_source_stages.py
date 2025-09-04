"""
An example demonstrating the use of source stages.
"""

from lijnding.core import stage

# --- Defining Source Stages ---

# A source stage is any stage created from a function that takes no input arguments
# (besides an optional `context` object). It is used to start a pipeline
# or to replace a data stream mid-pipeline.


@stage
def number_generator():
    """A simple source stage that yields numbers."""
    print("--- Source stage `number_generator` is running ---")
    yield from range(5)


@stage
def string_generator():
    """Another source stage that yields strings."""
    print("--- Source stage `string_generator` is running ---")
    yield "a"
    yield "b"


# --- Using Source Stages ---


@stage
def multiply_by_ten(x: int):
    return x * 10


@stage
def to_upper(text: str):
    return text.upper()


def example_source_at_start():
    """Demonstrates a pipeline that begins with a source stage."""
    print("--- Example 1: A pipeline starting with a source stage ---")

    # This pipeline starts with `number_generator`, so it creates its own data.
    pipeline = number_generator | multiply_by_ten

    # When a pipeline starts with a source stage, any data passed to `collect()`
    # is ignored. You can pass `None` or an empty list.
    results, _ = pipeline.collect()
    print("Result:", results)
    print("Expected: [0, 10, 20, 30, 40]")
    print("-" * 20)


def example_source_in_middle():
    """Demonstrates a source stage replacing a stream mid-pipeline."""
    print("\n--- Example 2: A pipeline with an in-line source ---")

    # When a source stage appears in the middle of a pipeline, it discards
    # the incoming stream and replaces it with its own output.
    pipeline = multiply_by_ten | string_generator | to_upper

    # The input `[1, 2]` goes into `multiply_by_ten`, but its output is
    # discarded when `string_generator` takes over.
    results, _ = pipeline.collect([1, 2])
    print("Result:", results)
    print("Expected: ['A', 'B']")
    print("-" * 20)


def main():
    """Runs the source stage examples."""
    example_source_at_start()
    example_source_in_middle()


if __name__ == "__main__":
    main()
