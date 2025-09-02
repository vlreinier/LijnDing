"""
An example demonstrating the use of aggregator components like `batch` and `reduce`.
"""
from lijnding import stage, aggregator_stage, batch, reduce

# --- A simple pipeline demonstrating batching and reducing ---

@stage
def generate_numbers():
    """A source stage to generate a stream of numbers."""
    print("Generating numbers 0 through 9...")
    yield from range(10)

# The `batch` component is an aggregator. It will collect items from the
# previous stage and group them into lists of a specified size.
batch_stage = batch(size=4)

# The `reduce` component is also an aggregator. It will receive the entire
# stream from the previous stage (in this case, a stream of lists)
# and reduce it to a single value.
@aggregator_stage
def sum_of_batches(batches):
    """Takes an iterable of batches and sums the numbers in each batch."""
    for batch_list in batches:
        print(f"Processing batch: {batch_list} -> Sum: {sum(batch_list)}")
        yield sum(batch_list)

# This reduce stage will take the stream of sums and add them all together.
reduce_stage = reduce(lambda a, b: a + b)


def main():
    # The pipeline generates numbers, batches them, sums each batch,
    # and then sums the sums.
    pipeline = generate_numbers | batch_stage | sum_of_batches | reduce_stage

    results, _ = pipeline.collect([]) # No input data needed due to source stage

    print("\n--- Final Result ---")
    # The sum of numbers from 0 to 9 is 45.
    print(f"Total sum: {results[0]}")
    assert results[0] == 45


if __name__ == "__main__":
    main()
