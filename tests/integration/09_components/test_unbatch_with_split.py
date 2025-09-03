import pytest
from lijnding.core.pipeline import Pipeline
from lijnding.components.batch import batch
from lijnding.components.split import split

def test_split_as_unbatch():
    """
    This test verifies that the `split` component can be used to 'unbatch'
    items, confirming that a separate `unbatch` component is not needed.
    """
    # The pipeline first batches items into lists of 3, then uses split
    # to flatten the stream of lists back into a stream of items.
    pipeline = Pipeline([
        batch(size=3),
        split() # With no arguments, split will iterate over the incoming item.
    ])

    input_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # The pipeline will produce:
    # 1. Batches: [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]
    # 2. Split (unbatched): 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

    result, _ = pipeline.collect(input_data)

    assert result == input_data
