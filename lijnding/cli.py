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
def run(
    pipeline_path: str,
    config_path: Optional[str],
    input_file,
    output_file,
):
    """
    Run a LijnDing pipeline.

    PIPELINE_PATH is the path to the pipeline object, e.g., 'my_project.pipelines:my_pipeline'.
    """
    try:
        pipeline = _load_pipeline(pipeline_path)
    except click.BadParameter as e:
        raise click.ClickException(str(e))

    # Read input lines and strip trailing newlines
    input_data = (line.rstrip("\n") for line in input_file)

    # Run the pipeline
    results_stream, _ = pipeline.run(input_data, config_path=config_path)

    # Write results to the output
    for result in results_stream:
        output_file.write(str(result) + "\n")


if __name__ == "__main__":
    cli()
