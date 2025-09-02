# LijnDing GUI and Smart Persistence

This document explains how to use the new graphical user interface (GUI) and the underlying "smart persistence" layer.

## Overview

The LijnDing framework now includes an optional web-based GUI for monitoring pipeline runs in real-time. This is made possible by a new persistence layer that automatically logs the state and events of a pipeline to disk.

-   **Headless by Default**: The framework runs without a GUI by default.
-   **Opt-in GUI**: The GUI and its persistence layer are only activated when you use the `--gui` flag.
-   **Separate Processes**: The GUI runs in a separate process from the pipeline and they do not share memory or context. Communication happens exclusively through the files written by the persistence layer.
-   **Resilience**: Failures in the GUI or its backend will not crash the running pipeline.

## How to Use the GUI

### 1. Installation

The GUI consists of two main packages: a FastAPI backend and a Svelte frontend. For now, they are included in the source tree under the `packages/` directory. To install their dependencies, you would navigate to their respective directories and run:

```bash
# For the backend
cd packages/lijnding-fastapi
pip install -e .

# For the frontend
cd packages/lijnding-svelte
npm install
```

### 2. Running the GUI

The GUI requires two components to be running:

1.  **The FastAPI Backend**: This server monitors the log files and provides data to the frontend.
    ```bash
    # From the root of the project
    cd packages/lijnding-fastapi
    uvicorn lijnding_fastapi.main:app --reload
    ```
    The backend will be available at `http://127.0.0.1:8000`.

2.  **The Svelte Frontend**: This is the web interface you interact with.
    ```bash
    # From the root of the project
    cd packages/lijnding-svelte
    npm run dev
    ```
    The frontend will be available at `http://localhost:5173`.

### 3. Running a Pipeline with GUI Monitoring

To have a pipeline show up in the GUI, run it from the command line with the `--gui` flag:

```bash
# Example using a pipeline from the examples directory
lijnding run examples.01_basics.01_simple_pipeline:shouting_pipeline --gui
```

When you run this command, a new directory will be created in `.lijnding_runs/` containing the logs for this specific run. The GUI will detect this new run and display it on the main page.

## The Smart Persistence Layer

The persistence layer is the foundation of the GUI. When `--gui` is enabled, a set of `PersistenceHooks` is attached to every stage in your pipeline.

### Directory Structure

All data is stored in the `.lijnding_runs/` directory at the root of your project.

```
.lijnding_runs/
└── <run_id_1>/
    └── events.log
└── <run_id_2>/
    └── events.log
...
```

-   **`<run_id>`**: A unique UUID assigned to each pipeline execution.
-   **`events.log`**: A file containing a stream of JSON objects, where each object represents an event in the pipeline's lifecycle (e.g., `pipeline_start`, `stage_item_complete`, `stage_item_error`).

This structured, file-based approach ensures a clean separation between the pipeline and the monitoring tools, enhancing stability and allowing for post-mortem analysis.
