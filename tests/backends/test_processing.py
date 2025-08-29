import pytest
from lijnding import Pipeline, stage, Context

# --- Test Functions for Processing ---
def _process_worker_func(x):
    return x * 2

def _process_context_func(context: Context, x):
    context.inc("proc_counter")
    return x

@pytest.mark.skip(reason="ProcessingRunner is unstable and has been disabled.")
def test_processing_backend_execution():
    process_stage = stage(backend="process", workers=2)(_process_worker_func)
    pipeline = Pipeline([process_stage])
    with pytest.raises(NotImplementedError):
        pipeline.collect([1, 2, 3, 4])

@pytest.mark.skip(reason="ProcessingRunner is unstable and has been disabled.")
def test_processing_backend_context():
    process_context_stage = stage(backend="process", workers=2)(_process_context_func)
    pipeline = Pipeline([process_context_stage])
    with pytest.raises(NotImplementedError):
        pipeline.collect([1, 2, 3, 4])
