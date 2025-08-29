"""
An example demonstrating the use of source stages.
"""
from lijnding import stage

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


def main():
    # --- Example 1: A pipeline starting with a source stage ---
    print("--- Running pipeline that starts with a source ---")
    # The input to `collect` will be ignored because the pipeline starts with a source.
    pipeline1 = number_generator | multiply_by_ten
    results1, _ = pipeline1.collect(["some", "ignored", "data"])
    print("Result 1:", results1) # Expected: [0, 10, 20, 30, 40]
    print("-" * 20)

    # --- Example 2: A source stage in the middle of a pipeline ---
    print("\n--- Running pipeline with an in-line source ---")
    # The output of `multiply_by_ten` will be discarded, and the stream will
    # be replaced by the output of `string_generator`.
    pipeline2 = multiply_by_ten | string_generator | to_upper
    results2, _ = pipeline2.collect([1, 2])
    print("Result 2:", results2) # Expected: ['A', 'B']
    print("-" * 20)


if __name__ == "__main__":
    main()
