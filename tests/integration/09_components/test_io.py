import pytest
from lijnding import Pipeline, stage
from lijnding.components import read_from_file, write_to_file, save_progress

def test_read_write_file(tmp_path):
    """
    Tests a simple pipeline that reads from a file, processes the data,
    and writes to another file.
    """
    input_file = tmp_path / "input.txt"
    output_file = tmp_path / "output.txt"

    lines = ["hello", "world", "lijnding"]
    input_file.write_text("\n".join(lines))

    @stage
    def to_upper(text: str):
        return text.upper()

    pipeline = read_from_file(str(input_file)) | to_upper | write_to_file(str(output_file))

    # Run the pipeline and collect the (empty) results to ensure it executes completely.
    results, _ = pipeline.collect([])
    assert not results # write_to_file is a terminal stage

    # Verify the output file content
    content = output_file.read_text().strip().split("\n")
    assert content == ["HELLO", "WORLD", "LIJNDING"]

def test_save_progress(tmp_path):
    """
    Tests the save_progress component to ensure it writes to a file and
    passes data through.
    """
    checkpoint_file = tmp_path / "checkpoint.txt"
    data = [1, 2, 3, 4, 5]

    @stage
    def double(x):
        return x * 2

    pipeline = double | save_progress(str(checkpoint_file))

    results, _ = pipeline.collect(data)

    # Check that the results are passed through correctly
    assert results == [2, 4, 6, 8, 10]

    # Check that the checkpoint file was written correctly
    content = checkpoint_file.read_text().strip().split("\n")
    assert content == ["2", "4", "6", "8", "10"]

def test_resume_from_checkpoint(tmp_path):
    """
    Tests a scenario where a pipeline is resumed from a checkpoint file.
    """
    checkpoint_file = tmp_path / "checkpoint.txt"
    output_file = tmp_path / "output.txt"

    # Assume a previous run created this checkpoint file
    checkpoint_data = ["apple", "banana", "cherry"]
    checkpoint_file.write_text("\n".join(checkpoint_data))

    @stage
    def add_suffix(text: str):
        return f"{text}-processed"

    # This pipeline resumes from the checkpoint
    resume_pipeline = (
        read_from_file(str(checkpoint_file))
        | add_suffix
        | write_to_file(str(output_file))
    )

    # Run the pipeline and collect the (empty) results to ensure it executes
    results, _ = resume_pipeline.collect([])
    assert not results

    content = output_file.read_text().strip().split("\n")
    assert content == ["apple-processed", "banana-processed", "cherry-processed"]
