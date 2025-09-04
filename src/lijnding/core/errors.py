from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .stage import Stage
    from .pipeline import Pipeline


class LijndingError(Exception):
    """Base class for all exceptions raised by the lijnding framework."""

    pass


class MissingDependencyError(LijndingError):
    """Raised when a component requires a dependency that is not installed."""

    pass


@dataclass
class ErrorPolicy:
    """
    Defines the error handling strategy for a pipeline Stage.

    Attributes:
        mode (str): The strategy to use when an error occurs.

            - **fail**: (Default) Stop execution and raise the exception.
            - **skip**: Ignore the item that caused the error and continue.
            - **retry**: Attempt to re-run the stage on the failing item.
            - **route_to_pipeline**: Send the failing item to a separate
              pipeline and pass on the transformed result.
            - **route_to_pipeline_and_retry**: Send the failing item to a
              separate pipeline and retry the original stage with the
              transformed result.

        retries (int): The number of times to retry if mode is 'retry' or
            'route_to_pipeline_and_retry'.
        backoff (float): The number of seconds to wait between retries.
        route_to_pipeline (Optional[Union["Stage", "Pipeline"]]): The pipeline
            to which a failing item should be routed. Used by
            'route_to_pipeline' and 'route_to_pipeline_and_retry' modes.
    """

    mode: str = "fail"
    retries: int = 3
    backoff: float = 0.1
    route_to_pipeline: Optional[Union["Stage", "Pipeline"]] = None

    def __post_init__(self):
        valid_modes = [
            "fail",
            "skip",
            "retry",
            "route_to_pipeline",
            "route_to_pipeline_and_retry",
        ]
        if self.mode not in valid_modes:
            raise ValueError(f"ErrorPolicy mode must be one of {valid_modes}")

        if self.mode in ["retry", "route_to_pipeline_and_retry"] and self.retries <= 0:
            raise ValueError("Retries must be a positive integer for retry modes")

        if (
            self.mode in ["route_to_pipeline", "route_to_pipeline_and_retry"]
            and self.route_to_pipeline is None
        ):
            raise ValueError(
                "'route_to_pipeline' must be a Stage or Pipeline for routing modes"
            )


class PipelineConnectionError(LijndingError):
    """Raised when two stages cannot be connected due to a type mismatch."""

    def __init__(self, from_stage: "Stage", to_stage: "Stage", message: str):
        self.from_stage = from_stage
        self.to_stage = to_stage
        self.message = message
        super().__init__(
            f"Cannot connect stage '{from_stage.name}' to '{to_stage.name}': {message}\n"
            f"  - Output type of '{from_stage.name}': {from_stage.output_type}\n"
            f"  - Input type of '{to_stage.name}': {to_stage.input_type}"
        )


class MissingTypeHintError(LijndingError, TypeError):
    """Raised when a Stage is defined without necessary type hints."""

    pass
