import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger for the given name.
    Initializes a default handler if no handlers are configured for the root logger.
    """
    logger = logging.getLogger(name)

    # If the root logger has no handlers, add a basic one so that messages are visible.
    # This prevents the "No handlers could be found for logger..." message.
    if not logging.getLogger().handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

    return logger
