import logging
import pytest
from logging.handlers import BufferingHandler

from lijnding import Pipeline, stage, Context
from tests.helpers.test_runner import run_pipeline, BACKENDS

class MemoryHandler(BufferingHandler):
    """A logging handler that stores records in memory."""
    def __init__(self, capacity):
        super().__init__(capacity)
        self.buffer = []

    def shouldFlush(self, record):
        return False # Never flush automatically

    def emit(self, record):
        self.buffer.append(self.format(record))

@pytest.fixture
def memory_handler():
    """A fixture that provides a memory handler and attaches it to the root logger."""
    handler = MemoryHandler(capacity=100)
    formatter = logging.Formatter('%(name)s:%(levelname)s:%(message)s')
    handler.setFormatter(formatter)

    root_logger = logging.getLogger("lijnding")
    root_logger.addHandler(handler)
    original_level = root_logger.level
    root_logger.setLevel(logging.DEBUG)

    yield handler

    # Teardown
    root_logger.removeHandler(handler)
    root_logger.setLevel(original_level)

@stage(name="test_logger_stage")
def logger_stage(context: Context, item: str):
    """A simple stage that logs a message using the context logger."""
    context.logger.info(f"Processing {item}")
    return item

@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_context_logger(backend, memory_handler):
    """
    Tests that `context.logger` is available and correctly namespaced for each backend.
    """
    # We need to re-decorate the stage for each backend to test them correctly
    @stage(name="test_logger_stage", backend=backend)
    def logger_stage_for_backend(context: Context, item: str):
        context.logger.info(f"Processing {item}")
        return item

    pipeline = Pipeline([logger_stage_for_backend])

    await run_pipeline(pipeline, ["a", "b"])

    # Verify that the log messages were captured and have the correct logger name
    expected_log_name = "lijnding.stage.test_logger_stage"

    # Filter for messages from our specific logger
    logged_messages = [msg for msg in memory_handler.buffer if msg.startswith(expected_log_name)]

    # The number of messages can vary depending on the backend (e.g., process starts more logs)
    # So we check for the specific messages we expect.
    assert f"{expected_log_name}:INFO:Processing a" in logged_messages
    assert f"{expected_log_name}:INFO:Processing b" in logged_messages
