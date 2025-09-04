import subprocess
import sys
import os
from pathlib import Path


# Get the path to the current Python executable
PYTHON_EXECUTABLE = sys.executable

# Define the project root to help with path resolution
PROJECT_ROOT = Path(__file__).parent.parent.parent
EXAMPLES_DIR = PROJECT_ROOT / "examples"


def run_cli_command(args: list[str]) -> subprocess.CompletedProcess:
    """Helper function to run the CLI command via `python -m`."""
    command = [PYTHON_EXECUTABLE, "-m", "lijnding.cli"] + args
    # Setting PYTHONPATH to ensure the local `lijnding` package is found
    env = os.environ.copy()
    # Add the project's `src` directory to PYTHONPATH, and the project root
    # for the examples
    python_path = f"{PROJECT_ROOT / 'src'}{os.pathsep}{PROJECT_ROOT}"
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{python_path}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = python_path

    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )


def test_cli_simple_pipeline_stdin_stdout():
    """
    Tests running a simple pipeline with input from stdin and output to stdout.
    """
    pipeline_path = "examples.01_basics.01_simple_pipeline:pipeline"
    input_data = "Hello world. This is a test."

    # The pipeline from the example splits sentences, counts words, and returns dicts.
    # Expected output for "Hello world.": {'word_count': 2, 'sentence': 'Hello world'}
    # Expected output for "This is a test.": {'word_count': 4, 'sentence': 'This is a test'}
    expected_output = (
        "{'word_count': 2, 'sentence': 'Hello world'}\n"
        "{'word_count': 4, 'sentence': 'This is a test'}\n"
    )

    # We can't directly run the CLI and pipe stdin easily, so we'll use a file for input.
    # A more advanced test setup might mock stdin.
    input_file = Path("test_cli_input.txt")
    input_file.write_text(input_data)

    try:
        result = run_cli_command(
            [
                "run",
                pipeline_path,
                "--input-file",
                str(input_file),
            ]
        )
        assert result.stdout == expected_output
    finally:
        input_file.unlink()  # Clean up the temp file


def test_cli_with_config_and_output_file():
    """
    Tests running a pipeline with a config file and writing to an output file.
    """
    pipeline_path = "examples.04_advanced.07_configuration:pipeline"
    config_file = EXAMPLES_DIR / "config" / "default.yml"
    input_data = "cli_test\n"

    input_file = Path("test_cli_input_config.txt")
    input_file.write_text(input_data)
    output_file = Path("test_cli_output.txt")

    try:
        # Run the command
        run_cli_command(
            [
                "run",
                pipeline_path,
                "--config",
                str(config_file),
                "--input-file",
                str(input_file),
                "--output-file",
                str(output_file),
            ]
        )

        # Check the output file content
        # The config adds "Hello from config, " and "!!!"
        expected_content = "Hello from config, cli_test!!!\n"
        actual_content = output_file.read_text()

        assert actual_content == expected_content

    finally:
        # Clean up temporary files
        input_file.unlink()
        if output_file.exists():
            output_file.unlink()
