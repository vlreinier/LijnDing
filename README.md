# lijnding: A Composable Pipeline Framework

lijnding is a lightweight, type-aware, and composable pipeline framework for Python. It allows you to build complex data processing workflows by chaining together simple, reusable stages.

## Core Concepts

- **Stage**: The fundamental building block of a pipeline. A stage is a simple Python function decorated with `@stage`. It performs a single, specific operation on data flowing through the pipeline.
- **Pipeline**: A sequence of stages connected by the `|` (pipe) operator. Data flows from one stage to the next. A `Pipeline` can also be treated as a single `Stage`, allowing you to nest pipelines.
- **Component**: A pre-built, configurable stage, usually created by a factory function (e.g., `branch()`, `batch()`). Components handle common tasks like branching, batching, and reducing.
- **Backend**: The execution engine for a stage. LijnDing supports different backends (`serial`, `thread`) for different needs. This is configured via the `@stage` decorator (e.g., `@stage(backend="thread")`).

## Key Features

- **Simple & Composable API**: Use the `@stage` decorator and the `|` operator to build elegant, readable pipelines.
- **Multiple Backends**: Run your pipelines serially, in a thread pool for I/O-bound tasks, or in a process pool for CPU-bound tasks.
- **Integrated Logging**: The framework emits detailed logs for pipeline and stage events, configurable through Python's standard `logging` module. Access the logger from your stages via `context.logger`.
- **Rich Component Library**: Includes pre-built components for filtering, file I/O, HTTP requests, branching, and more.
- **Checkpointing**: Use the `save_progress` and `read_from_file` components to easily checkpoint and resume long-running pipelines.
- **Extensible**: Add your own execution backends with the `register_backend` function.
- **Error Handling**: Configure how your pipeline behaves on errors with policies like `fail`, `skip`, or `retry`.
- **Nestable Pipelines**: Encapsulate and reuse complex workflows by using a pipeline as a stage within another pipeline.

## Basic Usage

Building a pipeline is as simple as defining stages and connecting them with `|`.

```python
from lijnding import stage

# 1. Define stages
@stage
def to_upper(text: str):
    return text.upper()

@stage
def exclaim(text: str):
    return f"{text}!"

# 2. Compose stages into a pipeline
#    You don't need to create a Pipeline() object first.
shouting_pipeline = to_upper | exclaim

# 3. Run the pipeline and collect the results
results, _ = shouting_pipeline.collect(["hello", "world"])

#
# results will be: ['HELLO!', 'WORLD!']
#
```

## Advanced Usage: Branching

Use the `branch()` component to run multiple operations on the same data.

```python
from lijnding import stage, branch

@stage
def get_length(text: str):
    return len(text)

# This pipeline will create tuples containing the uppercase version
# and the length for each input word.
# The `branch` component is seamlessly integrated with the `|` operator.
analysis_pipeline = branch(to_upper, get_length, merge="zip")

results, _ = analysis_pipeline.collect(["hello", "lijnding"])

#
# results will be: [('HELLO', 5), ('LIJNDING', 8)]
#
```

---

## Installation

To install the framework from source, clone the repository and run the following command in the project root:

```bash
pip install .
```

### Optional Dependencies

Some components require extra dependencies. You can install them as needed:

- **HTTP Component**: To use the `http_request` component, install the `[http]` extra:
  ```bash
  pip install .[http]
  ```

## Development

To set up the project for development, it's recommended to create a virtual environment and install the package in editable mode with its test dependencies.

```bash
# Create and activate a virtual environment (e.g., using venv)
python -m venv .venv
source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`

# Install the project in editable mode with test dependencies
pip install -e .[test]
```

### Running Tests

To run the test suite, use `pytest`:

```bash
python -m pytest
```

## Creating Custom Components

A component is a factory function that returns a configured `Stage`. This pattern allows you to create reusable, configurable pieces of your pipeline.

Here is an example of a simple component that adds a prefix to a string:

```python
from typing import Generator
from lijnding import stage, Stage, Pipeline

def add_prefix(prefix: str, **stage_kwargs) -> Stage:
    """
    A component that adds a prefix to each incoming string.

    :param prefix: The string prefix to add.
    :param stage_kwargs: Allows passing arguments like `backend` to the stage.
    :return: A configured Stage.
    """
    @stage(name=f"add_prefix_{prefix}", **stage_kwargs)
    def _prefix_stage(item: str) -> Generator[str, None, None]:
        yield f"{prefix}{item}"

    return _prefix_stage

# --- Usage ---

# Create a configured instance of the component
add_hello = add_prefix("Hello, ")

# Use it in a pipeline
pipeline = add_hello | stage(lambda s: s.upper())

# Run the pipeline
results, _ = pipeline.collect(["world", "Jules"])

# results will be: ['HELLO, WORLD', 'HELLO, JULES']
print(results)
```
