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
- **Modular and Extensible**: The framework is split into a core package and optional components that can be installed separately.
- **Web-Based GUI**: An optional, standalone web interface for real-time monitoring of pipeline runs.

## Architecture

LijnDing is designed as a modular, multi-package framework. This allows users to install only the components they need. The main packages are:

- **`lijnding-core`**: The essential pipeline framework.
- **`lijnding-http`**: Provides HTTP components.
- **`lijnding-rabbitmq`**: Provides RabbitMQ components.
- **`lijnding-ui`**: A standalone web interface for monitoring.

## GUI and Monitoring

The web-based GUI is provided as a separate package, `lijnding-ui`. For installation and usage instructions, please see the [README in the `ui/` directory](./ui/README.md).

## Basic Usage

Building a pipeline is as simple as defining stages and connecting them with `|`.

```python
from lijnding.core import stage

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

## Advanced Usage

### Branching

The `branch` component is a powerful tool for creating complex, non-linear workflows. It allows you to send a single input item to multiple child branches, and then merges the results back into a single stream.

**Key Concept**: Each item that enters a `branch` stage is passed to *all* of its child branches. The branches do not receive the output of the previous branch; they all receive the same input item.

The `branch` component has two merge strategies:

- `concat`: Flattens the results from all branches into a single list.
- `zip`: Groups the results for each input item into a tuple.

Here is an example that demonstrates both strategies:

```python
from lijnding.core import stage
from lijnding.components import branch

@stage
def to_upper(text: str): return text.upper()

@stage
def get_length(text: str): return len(text)

@stage
def reverse_text(text: str): return text[::-1]

data = ["hello", "world"]

# Using 'zip' to create tuples of results
zip_pipeline = branch(to_upper, get_length, reverse_text, merge="zip")
results_zip, _ = zip_pipeline.collect(data)
# results_zip is: [('HELLO', 5, 'olleh'), ('WORLD', 5, 'dlrow')]

# Using 'concat' to create a flat list of results
concat_pipeline = branch(to_upper, get_length, reverse_text, merge="concat")
results_concat, _ = concat_pipeline.collect(data)
# results_concat is: ['HELLO', 5, 'olleh', 'WORLD', 5, 'dlrow']
```

### Aggregators

In a standard pipeline, stages process items one by one. However, some operations need to work on the entire stream of items at once. These are called **aggregator stages**.

There are two main ways to use aggregators:

1.  **Built-in Components**: The framework provides several pre-built aggregator components, such as `batch`, which groups items into lists, and `reduce`, which combines all items into a single result.

2.  **The `@aggregator_stage` Decorator**: For custom logic, you can create your own aggregator stage by decorating a function with `@aggregator_stage`. This decorator modifies your function so that instead of receiving items one by one, it receives a single argument: an `Iterable` containing all items from the previous stage.

Here is an example that uses both a built-in aggregator (`batch`) and a custom one (`process_batches`):

```python
from lijnding.core import stage, aggregator_stage
from lijnding.components import batch, reduce

@stage
def generate_numbers():
    yield from range(10)

# `process_batches` is a custom aggregator that calculates the sum of each batch
@aggregator_stage
def process_batches(batches):
    for batch_list in batches:
        yield sum(batch_list)

# `reduce` is a built-in aggregator that sums the results from the previous stage
reduce_stage = reduce(lambda a, b: a + b, initializer=0)

# The pipeline generates numbers, batches them, sums each batch, and then sums the sums.
pipeline = generate_numbers | batch(size=4) | process_batches | reduce_stage

results, _ = pipeline.collect([])
# results is: [45]
```

### Using External Configuration

For more complex pipelines, it's good practice to separate your parameters from your code. LijnDing supports loading pipeline parameters from a YAML file.

**1. Create a YAML file** (e.g., `config.yml`):

```yaml
# config.yml
processing:
  greeting: "Hello from config"
  punctuation: "!"
```

**2. Access config from your stages**:

The `context` object provides access to a `config` object. You can use its `.get()` method to retrieve values. The `get()` method allows you to provide a default value for graceful fallback.

```python
from lijnding.core import stage

@stage
def add_greeting(context, text: str) -> str:
    # Access a nested value, providing a default if it's not found
    greeting = context.config.get("processing.greeting", "Hi")
    return f"{greeting}, {text}"

@stage
def punctuate(context, text: str) -> str:
    punc = context.config.get("processing.punctuation", ".")
    return f"{text}{punc}"
```

**3. Run the pipeline with configuration**:

Pass the path to your YAML file to the `run()` or `collect()` method using the `config_path` argument.

```python
pipeline = add_greeting | punctuate

# Run with the config file
results, _ = pipeline.collect(
    ["world"],
    config_path="config.yml"
)
# results is: ['Hello from config, world!']

# Run without the config file (uses defaults)
results_no_config, _ = pipeline.collect(["world"])
# results_no_config is: ['Hi, world.']
```

---

## Installation

The core framework can be installed from the root of the repository:
```bash
pip install .
```

Optional components can be installed from their respective directories:
```bash
# Install HTTP components
pip install ./components/http

# Install RabbitMQ components
pip install ./components/rabbitmq

# Install the Web UI
pip install ./ui
```

## Development

To set up a complete development environment with all components and test dependencies, it's recommended to create a virtual environment and install all packages in editable mode.

**Note**: You must install the core package, all components, and the test dependencies to run the full test suite.

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`

# 2. Install all packages in editable mode
#    This includes the core framework, all optional components, and test dependencies.
pip install -e .[test]
pip install -e ./components/http
pip install -e ./components/rabbitmq
pip install -e ./ui
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
from lijnding.core import stage, Stage, Pipeline

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
