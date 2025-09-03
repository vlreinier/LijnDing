# Core Concepts

Lijnding is built around a few core concepts that work together to create powerful and flexible data pipelines.

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
