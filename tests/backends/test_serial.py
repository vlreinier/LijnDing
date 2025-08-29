from lijnding import Pipeline, stage

def test_serial_backend_with_aggregator():
    @stage(stage_type="aggregator")
    def sum_all(iterable):
        yield sum(iterable)

    pipeline = Pipeline([sum_all])
    results, _ = pipeline.collect([1, 2, 3, 4])
    assert results == [10]
