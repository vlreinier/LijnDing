import logging
import pytest
import json
from logging import LogRecord

import structlog

from lijnding import Pipeline, stage, Context
from tests.helpers.test_runner import run_pipeline, BACKENDS

class MemoryHandler(logging.Handler):
    """A logging handler that stores records in memory."""
    def __init__(self):
        super().__init__()
        self.records = []
        self.buffer = []

    def emit(self, record: LogRecord):
        self.records.append(record)
        self.buffer.append(record.getMessage())

@pytest.fixture
def memory_handler():
    """
    A fixture that provides a memory handler and attaches it to the root logger
    for capturing raw log records.
    """
    # This import is here to ensure structlog is configured before we add our handler
    from lijnding.core.log import get_logger

    handler = MemoryHandler()

    # Attach to the root logger used by structlog's default PrintLoggerFactory
    logger = logging.getLogger()

    original_handlers = logger.handlers[:]
    logger.handlers = [handler]  # Replace handlers to exclusively capture output

    yield handler

    # Teardown
    logger.handlers = original_handlers


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_context_logger(backend):
    """
    Tests that `context.logger` is available and correctly namespaced for each backend,
    and that it produces structured logs.
    """
    # We need a separate fixture for this test as it modifies the logging setup
    handler = MemoryHandler()
    logger = logging.getLogger()
    original_handlers = logger.handlers[:]
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)

    # Since we removed automatic configuration from the library, we must configure
    # it for this test to capture the logs.
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False, # Avoid caching between test runs
    )

    try:
        @stage(name="test_logger_stage", backend=backend, workers=2 if backend == 'process' else 1)
        def logger_stage_for_backend(context: Context, item: str):
            context.logger.info(f"processing_item", item_id=item)
            return item

        pipeline = Pipeline([logger_stage_for_backend])
        await run_pipeline(pipeline, ["a", "b"])

        # The log output is now a JSON string per line
        # We need to parse it to inspect the contents.
        parsed_logs = [json.loads(msg) for msg in handler.buffer]

        # Check for item-specific logs from the context logger
        item_logs = [
            log for log in parsed_logs
            if log.get("event") == "processing_item" and log.get("logger") == "lijnding.stage.test_logger_stage"
        ]

        # The process backend does not share memory and its logs are not captured by the handler.
        if backend != "process":
            assert len(item_logs) == 2, f"Expected 2 item logs, but found {len(item_logs)}"
            assert {"a", "b"} == {log.get("item_id") for log in item_logs}

        # Check that the stream lifecycle logs were also created by the correct logger
        stream_started_log = next((log for log in parsed_logs if log.get("event") == "stream_started"), None)
        stream_finished_log = next((log for log in parsed_logs if log.get("event") == "stream_finished"), None)

        assert stream_started_log is not None, "stream_started log not found"
        assert stream_finished_log is not None, "stream_finished log not found"

        assert stream_started_log.get("logger") == "lijnding.stage.test_logger_stage"
        assert stream_finished_log.get("logger") == "lijnding.stage.test_logger_stage"

    finally:
        # Restore original handlers
        logger.handlers = original_handlers
