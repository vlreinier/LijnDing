from typing import Generator, Any, Iterable

from ..core.stage import stage, Stage


def read_from_file(filepath: str, *, name: str = "read_from_file", **stage_kwargs) -> Stage:
    """
    Creates a source stage that reads a file and yields each line.

    This is a source component, meaning it should be the first stage in a
    pipeline and does not require an input stream.

    :param filepath: The path to the file to be read.
    :param name: An optional name for the stage.
    :param stage_kwargs: Additional keyword arguments to pass to the @stage decorator.
    :return: A new source `Stage` that yields lines from the file.
    """
    @stage(name=name, stage_type="source", **stage_kwargs)
    def _read_from_file_stage() -> Generator[str, None, None]:
        """The actual stage function that reads the file."""
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                yield line.strip()

    return _read_from_file_stage


def write_to_file(filepath: str, *, name: str = "write_to_file", end_of_line: str = "\n", **stage_kwargs) -> Stage:
    """
    Creates a terminal stage that writes all incoming items to a file.

    This is a terminal component, meaning it consumes items but does not
    yield anything. It can be placed at the end of a pipeline.

    :param filepath: The path to the file to be written.
    :param name: An optional name for the stage.
    :param end_of_line: The character(s) to write after each item.
    :param stage_kwargs: Additional keyword arguments to pass to the @stage decorator.
    :return: A new `Stage` that writes items to the file.
    """
    # This stage is an aggregator because it needs to control the file resource
    # over the entire stream of items.
    @stage(name=name, stage_type="aggregator", **stage_kwargs)
    def _write_to_file_stage(items: Iterable[Any]) -> None:
        """The actual stage function that writes to the file."""
        with open(filepath, 'w', encoding='utf-8') as f:
            for item in items:
                f.write(str(item) + end_of_line)

    return _write_to_file_stage


def save_progress(filepath: str, *, name: str = "save_progress", end_of_line: str = "\n", **stage_kwargs) -> Stage:
    """
    Creates a pass-through stage that writes each incoming item to a file
    and then yields the item downstream. This is useful for checkpointing.

    This component acts like an aggregator to efficiently manage the file
    handle, but it yields items back into the pipeline stream.

    :param filepath: The path to the checkpoint file. The file will be appended to.
    :param name: An optional name for the stage.
    :param end_of_line: The character(s) to write after each item.
    :param stage_kwargs: Additional keyword arguments to pass to the @stage decorator.
    :return: A new `Stage` that saves progress and passes items through.
    """
    @stage(name=name, stage_type="aggregator", **stage_kwargs)
    def _save_progress_stage(items: Iterable[Any]) -> Generator[Any, None, None]:
        """The actual stage function that writes to the file and yields."""
        with open(filepath, 'a', encoding='utf-8') as f:
            for item in items:
                f.write(str(item) + end_of_line)
                yield item

    return _save_progress_stage
