"""
Command Line Interface for the LijnDing framework.
"""
import importlib
import sys
import logging
from typing import Optional

import click
import structlog

from .core.pipeline import Pipeline


def _load_pipeline(path: str) -> Pipeline:
    """Dynamically loads a pipeline object from a module path."""
    try:
        module_path, object_name = path.split(":", 1)
    except ValueError:
        raise click.BadParameter(
            "Pipeline path must be in the format 'path.to.module:pipeline_variable'"
        )

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise click.BadParameter(f"Could not import module '{module_path}': {e}")

    if not hasattr(module, object_name):
        raise click.BadParameter(
            f"Module '{module_path}' does not have a variable named '{object_name}'"
        )

    pipeline = getattr(module, object_name)
    if not isinstance(pipeline, Pipeline):
        raise click.BadParameter(
            f"Object '{object_name}' in '{module_path}' is not a LijnDing Pipeline instance."
        )

    return pipeline


def _configure_logging():
    """Configures logging for the CLI application."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger = logging.getLogger()
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


@click.group()
def cli():
    """LijnDing command-line interface."""
    _configure_logging()


@cli.command()
@click.argument("pipeline_path", type=str)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to a YAML configuration file.",
)
@click.option(
    "--input-file",
    "input_file",
    type=click.File("r"),
    default=sys.stdin,
    help="Path to a file to read input from (reads from stdin by default).",
)
@click.option(
    "--output-file",
    "output_file",
    type=click.File("w"),
    default=sys.stdout,
    help="Path to a file to write output to (writes to stdout by default).",
)
@click.option(
    "--gui",
    "is_gui_enabled",
    is_flag=True,
    default=False,
    help="Enable the persistence layer for monitoring with the LijnDing GUI.",
)
def run(
    pipeline_path: str,
    config_path: Optional[str],
    input_file,
    output_file,
    is_gui_enabled: bool,
):
    """
    Run a LijnDing pipeline.

    PIPELINE_PATH is the path to the pipeline object, e.g., 'my_project.pipelines:my_pipeline'.
    """
    try:
        pipeline = _load_pipeline(pipeline_path)
    except click.BadParameter as e:
        raise click.ClickException(str(e))

    if is_gui_enabled:
        try:
            from lijnding_gui.persistence import PersistenceHooks
        except ImportError:
            raise click.ClickException(
                "The 'lijnding_gui' package is not installed. "
                "Please install it to use the --gui feature."
            )

        hooks = PersistenceHooks()
        # The on_worker_init hook will create the run directory.
        # We need to do a bit of a hack to get the run_id and log_dir
        # before the pipeline starts, so we can configure the logger.
        # We'll create the state manually here. This is a bit of a code smell
        # and could be refactored later.
        import uuid
        from pathlib import Path

        run_id = str(uuid.uuid4())
        log_dir = Path(hooks.base_log_dir) / run_id
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / "events.log"

        # Add a dedicated file handler for this run's events
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(message)s"))

        run_logger = logging.getLogger(f"lijnding.run.{run_id}")
        run_logger.addHandler(file_handler)
        run_logger.setLevel(logging.INFO)
        run_logger.propagate = False # Don't send events to the root logger

        # Now, we need to ensure the hooks use this run_id
        from lijnding_gui.persistence import State
        hooks.state = State(run_id=run_id, log_dir=log_dir)

        for stage in pipeline.stages:
            stage.hooks = hooks

    # Read input lines and strip trailing newlines
    input_data = (line.rstrip("\n") for line in input_file)

    # Run the pipeline
    results_stream, _ = pipeline.run(input_data, config_path=config_path)

    # Write results to the output
    # If the GUI is enabled, we might not want to write to stdout,
    # but for now, we'll keep this behavior.
    for result in results_stream:
        output_file.write(str(result) + "\n")


if __name__ == "__main__":
    cli()
