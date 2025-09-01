import pytest
from lijnding import Pipeline, stage, ErrorPolicy, Context

# --- Test Stages with Errors ---

class MyException(Exception):
    pass

@stage
def failing_stage(x):
    if x == 2:
        raise MyException("I failed on 2!")
    return x

# A stage to test retries
retry_counter = 0
@stage(error_policy=ErrorPolicy(mode="retry", retries=2))
def retry_stage(x):
    global retry_counter
    retry_counter += 1
    if x == 0:
        raise MyException("Retry me")
    return x

# --- Error Handling Tests ---

def test_error_policy_fail():
    pipeline = Pipeline() | failing_stage
    with pytest.raises(MyException):
        pipeline.collect([1, 2, 3])

def test_error_policy_skip():
    # Configure the stage to skip errors
    skipping_failing_stage = stage(error_policy=ErrorPolicy(mode="skip"))(failing_stage.func)

    pipeline = Pipeline() | skipping_failing_stage
    results, _ = pipeline.collect([1, 2, 3, 4])

    # The item '2' should be skipped
    assert results == [1, 3, 4]

    # Check that the error was recorded in the metrics
    error_stage = pipeline.stages[0]
    assert error_stage.metrics["errors"] == 1

def test_error_policy_retry():
    global retry_counter
    retry_counter = 0 # Reset counter

    pipeline = Pipeline() | retry_stage

    # This should fail eventually, but after retrying
    with pytest.raises(MyException):
        pipeline.collect([0])

    # The stage should have been called 3 times: 1 original + 2 retries
    assert retry_counter == 3

    # Reset for next test
    retry_counter = 0

    # Test that it passes if the input is not the failing one
    results, _ = pipeline.collect([1])
    assert results == [1]
    assert retry_counter == 1
