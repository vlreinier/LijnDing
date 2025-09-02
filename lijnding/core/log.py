import logging
import sys
from typing import Optional

# Attempt to import structlog
try:
    import structlog
    from structlog.types import EventDict, WrappedLogger
    _has_structlog = True
except ImportError:
    _has_structlog = False

# --- Structlog Configuration ---

def _configure_structlog():
    """Configures structlog to produce JSON-formatted logs via stdlib."""
    if not _has_structlog or structlog.is_configured():
        return

    # This is a fallback configuration. Users should configure logging themselves.
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    # Use a temporary name to avoid interfering with user's handlers
    handler.set_name("lijnding_fallback_handler")

    root_logger = logging.getLogger()
    # Avoid adding duplicate handlers
    if "lijnding_fallback_handler" not in [h.get_name() for h in root_logger.handlers]:
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

class StructlogLogger:
    """A compatibility wrapper for the structlog logger."""
    def __init__(self, name: str):
        # get_logger with a name will be passed to the LoggerFactory
        self._logger = structlog.get_logger(name)

    def info(self, event: str, **data):
        self._logger.info(event, **data)

    def warning(self, event: str, **data):
        self._logger.warning(event, **data)

    def error(self, event: str, **data):
        self._logger.error(event, **data)

    def debug(self, event: str, **data):
        self._logger.debug(event, **data)

    def bind(self, **new_values):
        return self._logger.bind(**new_values)

_structlog_configured = False

def get_logger(name: str) -> logging.Logger | StructlogLogger:
    """
    Returns a logger for the given name.

    If structlog is installed, it returns a configured structlog logger that
    is integrated with the standard library's logging system.
    Otherwise, it falls back to a standard Python logger.
    """
    global _structlog_configured
    if _has_structlog:
        if not _structlog_configured:
            _configure_structlog()
            _structlog_configured = True
        return StructlogLogger(name)
    else:
        # Fallback to standard logging if structlog is not available
        logger = logging.getLogger(name)
        if not logging.getLogger().handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logging.getLogger().addHandler(handler)
            logging.getLogger().setLevel(logging.INFO)
        return logger
