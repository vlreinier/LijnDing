"""
An example demonstrating the use of aggregator components like `batch` and `reduce`.
"""
from lijnding.core import stage, aggregator_stage
from lijnding.components.batch import batch
from lijnding.components.reduce import reduce

# --- A simple pipeline demonstrating batching and reducing ---

@stage
def generate_numbers():
    """A source stage to generate a stream of numbers."""
    print("Generating numbers 0 through 9...")
    yield from range(10)

# The `batch` component is an aggregator. It will collect items from the
# previous stage and group them into lists of a specified size.
batch_stage = batch(size=4)

# An aggregator stage is a stage that receives all items from the previous
# stage at once, as an iterable. This is in contrast to a regular stage, which
# receives items one by one.
#
# The `@aggregator_stage` decorator is used to mark a function as an aggregator.
# It can be used to implement custom aggregation logic.
#
# Note the difference between an aggregator *component* like `batch` or `reduce`
# (which are pre-built aggregators) and the `@aggregator_stage` *decorator*
# (which is a tool for creating your own aggregator stages).
@aggregator_stage
def process_batches(batches):
    """
    Takes an iterable of batches and yields the sum of each batch.
    This is an example of a custom aggregator stage.
    """
    for batch_list in batches:
        batch_sum = sum(batch_list)
        print(f"Processing batch: {batch_list} -> Sum: {batch_sum}")
        yield batch_sum

# The `reduce` component is another example of a built-in aggregator.
# It will take the stream of sums from the previous stage and add them
# all together to produce a single final result.
reduce_stage = reduce(lambda a, b: a + b, initializer=0)


def main():
    # The pipeline will:
    # 1. Generate numbers from 0 to 9.
    # 2. Batch them into lists of 4.
    # 3. Sum each batch using our custom aggregator.
    # 4. Sum the sums of the batches to get a final total.
    pipeline = generate_numbers | batch_stage | process_batches | reduce_stage

    results, _ = pipeline.collect([])  # No input data needed due to source stage

    print("\n--- Final Result ---")
    # The sum of numbers from 0 to 9 is 45.
    print(f"Total sum: {results[0]}")
    assert results[0] == 45


if __name__ == "__main__":
    main()
