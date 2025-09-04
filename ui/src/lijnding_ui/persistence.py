import uuid
import time
from pathlib import Path
from typing import Any, Dict, Optional

from lijnding.core.hooks import Hooks
from lijnding.core.log import get_logger
from lijnding.core.stage import Stage
from lijnding.core.context import Context


class State:
    """A simple class to hold the state of a pipeline run for persistence."""

    def __init__(self, run_id: str, log_dir: Path):
        self.run_id = run_id
        self.log_dir = log_dir
        self.logger = self._setup_logger()
        self.pipeline_name: Optional[str] = None
        self.start_time: Optional[float] = None

    def _setup_logger(self):
        """Configures a dedicated logger for the pipeline run."""
        # log_file = self.log_dir / "events.log"
        # This is a simplified logger setup.
        # In a real implementation, we would use structlog's file-based logging.
        # For now, we'll just get a standard logger.
        # The actual file writing will be handled by a file handler
        # configured in the CLI.
        return get_logger(f"lijnding.run.{self.run_id}")


class PersistenceHooks(Hooks):
    """
    A set of hooks that implement the 'smart persistence' layer.

    These hooks write structured logs for pipeline events to a dedicated
    run directory, allowing a separate GUI process to monitor and visualize
    the pipeline's state.
    """

    def __init__(self, base_log_dir: str = ".lijnding_runs"):
        self.base_log_dir = Path(base_log_dir)
        self.state: Optional[State] = None

    def on_worker_init(self, context: "Context") -> Dict[str, Any]:
        """
        Called when a worker (and thus a pipeline run) is initialized.
        """
        if not self.state:
            run_id = str(uuid.uuid4())
            log_dir = self.base_log_dir / run_id
            log_dir.mkdir(parents=True, exist_ok=True)
            self.state = State(run_id=run_id, log_dir=log_dir)
            self.state.start_time = time.time()
            self.state.pipeline_name = context.pipeline_name or "unknown_pipeline"

            self.state.logger.info(
                "pipeline_start",
                run_id=self.state.run_id,
                pipeline_name=self.state.pipeline_name,
                start_time=self.state.start_time,
            )
        return {"run_id": self.state.run_id}

    def before_stage(self, stage: "Stage", context: "Context", item: Any):
        """Called before a stage processes an item."""
        if not self.state:
            return

        self.state.logger.info(
            "stage_item_start",
            run_id=self.state.run_id,
            stage_name=stage.name,
            item=item,
        )

    def after_stage(
        self,
        stage: "Stage",
        context: "Context",
        item: Any,
        result: Any,
        time_taken: float,
    ):
        """Called after a stage successfully processes an item."""
        if not self.state:
            return

        self.state.logger.info(
            "stage_item_complete",
            run_id=self.state.run_id,
            stage_name=stage.name,
            item=item,
            result=result,
            time_taken=time_taken,
            metrics=stage.metrics,
        )

    def on_error(
        self,
        stage: "Stage",
        context: "Context",
        item: Any,
        error: BaseException,
        retries_left: int,
    ):
        """Called when a stage encounters an error."""
        if not self.state:
            return

        self.state.logger.error(
            "stage_item_error",
            run_id=self.state.run_id,
            stage_name=stage.name,
            item=item,
            error=str(error),
            retries_left=retries_left,
        )

    def on_worker_exit(self, context: "Context"):
        """Called when a worker is about to terminate."""
        if not self.state:
            return

        end_time = time.time()
        duration = end_time - (self.state.start_time or end_time)
        self.state.logger.info(
            "pipeline_end",
            run_id=self.state.run_id,
            pipeline_name=self.state.pipeline_name,
            end_time=end_time,
            duration=duration,
        )
        self.state = None
