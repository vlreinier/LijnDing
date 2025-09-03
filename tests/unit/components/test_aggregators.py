from lijnding.core import Pipeline
from lijnding.components.batch import batch
from lijnding.components.reduce import reduce_

def test_batch_component():
    pipeline = Pipeline([batch(size=2)])
    results, _ = pipeline.collect([1, 2, 3, 4])
    assert results == [[1, 2], [3, 4]]

    pipeline_partial = Pipeline([batch(size=3)])
    results_partial, _ = pipeline_partial.collect([1, 2, 3, 4, 5])
    assert results_partial == [[1, 2, 3], [4, 5]]

def test_reduce_component():
    pipeline = Pipeline([reduce_(lambda a, b: a + b, 100)])
    results, _ = pipeline.collect([1, 2, 3])
    assert results == [106]

    pipeline_no_init = Pipeline([reduce_(lambda a, b: a + b)])
    results_no_init, _ = pipeline_no_init.collect([1, 2, 3])
    assert results_no_init == [6]

    pipeline_empty, _ = pipeline_no_init.collect([])
    assert pipeline_empty == []
