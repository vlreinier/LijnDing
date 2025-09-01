import pytest
from lijnding import Pipeline, stage

def _a_func(x):
    return x

@pytest.mark.skip(reason="ProcessingRunner is unstable and has been disabled.")
def test_processing_backend_raises_not_implemented():
    """
    Tests that using the 'process' backend raises a NotImplementedError.
    """
    process_stage = stage(backend="process")(_a_func)
    pipeline = Pipeline([process_stage])

    with pytest.raises(NotImplementedError, match="unstable and disabled"):
        pipeline.collect([1, 2, 3])
