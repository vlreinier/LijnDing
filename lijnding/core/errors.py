from __future__ import annotations
from dataclasses import dataclass

@dataclass
class ErrorPolicy:
    """
    Defines the error handling strategy for a pipeline Stage.

    Attributes:
        mode (str): The strategy to use when an error occurs.
            - 'fail': (Default) Stop execution and raise the exception.
            - 'skip': Ignore the item that caused the error and continue.
            - 'retry': Attempt to re-run the stage on the failing item.
        retries (int): The number of times to retry if mode is 'retry'.
        backoff (float): The number of seconds to wait between retries.
                         The wait time is `backoff * attempt_number`.
    """
    mode: str = "fail"  # 'fail' | 'skip' | 'retry'
    retries: int = 0
    backoff: float = 0.0  # seconds linear backoff per attempt

    def __post_init__(self):
        if self.mode not in ["fail", "skip", "retry"]:
            raise ValueError("ErrorPolicy mode must be 'fail', 'skip', or 'retry'")
        if self.mode == "retry" and self.retries <= 0:
            raise ValueError("Retries must be a positive integer for 'retry' mode")
