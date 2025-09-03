import subprocess
import sys

import pytest

def run_example(name: str):
    """Runs an example script as a separate process and returns its output."""
    path = f"examples/{name}.py"
    process = subprocess.run(
        [sys.executable, path],
        capture_output=True,
        text=True,
        check=True,
        cwd="..",  # Run from the project root
    )
    return process.stdout

def test_simple_pipeline_example():
    """Tests the 01_basics/01_simple_pipeline.py example."""
    output = run_example("01_basics/01_simple_pipeline")
    assert "--- Pipeline Results ---" in output
    assert "{'word_count': 5, 'sentence': 'This is the first sentence'}" in output
    assert "{'word_count': 4, 'sentence': 'This is the second'}" in output
    assert "{'word_count': 6, 'sentence': 'Here is another block of text'}" in output
    assert "{'word_count': 5, 'sentence': 'It has two more sentences'}" in output

def test_context_and_metrics_example():
    """Tests the 01_basics/02_context_and_metrics.py example."""
    output = run_example("01_basics/02_context_and_metrics")
    assert "--- Metrics from on_stream_end Hook ---" in output
    assert "Total items processed: 4" in output
    assert "Items with errors: 1" in output
    assert "--- Final Result ---" in output
    assert "Sum of values: 60" in output

def test_source_stages_example():
    """Tests the 01_basics/03_source_stages.py example."""
    output = run_example("01_basics/03_source_stages")
    assert "--- Example 1: A pipeline starting with a source stage ---" in output
    assert "Result: [0, 10, 20, 30, 40]" in output
    assert "--- Example 2: A pipeline with an in-line source ---" in output
    assert "Result: ['A', 'B']" in output

def test_threading_backend_example():
    """Tests the 02_backends/01_threading_backend.py example."""
    output = run_example("02_backends/01_threading_backend")
    assert "--- Starting pipeline with 'thread' backend ---" in output
    assert "Downloaded 6 pages." in output
    # Check that the execution time is less than the sequential time (6 seconds)
    # This is a bit loose to avoid flaky tests, but it captures the spirit of the example.
    import re
    match = re.search(r"Total execution time: (\d+\.\d+) seconds", output)
    assert match is not None
    execution_time = float(match.group(1))
    assert execution_time < 6

def test_branching_example():
    """Tests the 03_components/01_branching.py example."""
    output = run_example("03_components/01_branching")
    assert "--- Running Branch with 'concat' merge strategy ---" in output
    assert "Result (a flattened list of all results):" in output
    assert "['HELLO', 'olleh', 5, 'WORLD', 'dlrow', 5, 'LIJNDING', 'gnidnjil', 8]" in output
    assert "--- Running Branch with 'zip' merge strategy ---" in output
    assert "Result (a list of tuples):" in output
    assert "[('HELLO', 5, 'olleh'), ('WORLD', 5, 'dlrow'), ('LIJNDING', 8, 'gnidnjil')]" in output

def test_aggregators_example():
    """Tests the 03_components/02_aggregators.py example."""
    output = run_example("03_components/02_aggregators")
    assert "--- Final Result ---" in output
    assert "Total sum: 45" in output
