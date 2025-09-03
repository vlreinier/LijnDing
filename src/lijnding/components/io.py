"""
This module provides file I/O components for reading from and writing to files
within a pipeline.
"""
from typing import Generator, Any, Iterable

from ..core.stage import stage, aggregator_stage, Stage


def read_from_file(filepath: str, *, name: str = "read_from_file", **stage_kwargs) -> Stage:
    """Creates a source stage that reads a file and yields each line.

    This is a source component, meaning it should be the first stage in a
    pipeline and does not require an input stream.

    Args:
        filepath: The path to the file to be read.
        name: An optional name for the stage.
        stage_kwargs: Additional keyword arguments to pass to the @stage decorator.

    Returns:
        A new source `Stage` that yields lines from the file.
    """
    @stage(name=name, stage_type="source", **stage_kwargs)
    def _read_from_file_stage() -> Generator[str, None, None]:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                yield line.strip()

    return _read_from_file_stage


def write_to_file(filepath: str, *, name: str = "write_to_file", end_of_line: str = "\n", **stage_kwargs) -> Stage:
    """Creates a terminal stage that writes all incoming items to a file.

    This is a terminal component, meaning it consumes items but does not
    yield anything. It can be placed at the end of a pipeline.

    Args:
        filepath: The path to the file to be written.
        name: An optional name for the stage.
        end_of_line: The character(s) to write after each item.
        stage_kwargs: Additional keyword arguments to pass to the @stage decorator.

    Returns:
        A new `Stage` that writes items to the file.
    """
    # This stage is an aggregator because it needs to control the file resource
    # over the entire stream of items.
    @aggregator_stage(name=name, **stage_kwargs)
    def _write_to_file_stage(items: Iterable[Any]) -> None:
        with open(filepath, 'w', encoding='utf-8') as f:
            for item in items:
                f.write(str(item) + end_of_line)

    return _write_to_file_stage


def save_progress(filepath: str, *, name: str = "save_progress", end_of_line: str = "\n", **stage_kwargs) -> Stage:
    """Creates a pass-through stage that writes each item to a file and then yields it.

    This is useful for checkpointing the progress of a pipeline. The file is
    opened in append mode, so it can be used across multiple runs to create a
    log of all processed items.

    Args:
        filepath: The path to the checkpoint file.
        name: An optional name for the stage.
        end_of_line: The character(s) to write after each item.
        stage_kwargs: Additional keyword arguments to pass to the @stage decorator.

    Returns:
        A new `Stage` that saves progress and passes items through.
    """
    @aggregator_stage(name=name, **stage_kwargs)
    def _save_progress_stage(items: Iterable[Any]) -> Generator[Any, None, None]:
        with open(filepath, 'a', encoding='utf-8') as f:
            for item in items:
                f.write(str(item) + end_of_line)
                yield item

    return _save_progress_stage
