# LijnDing FastAPI Backend

This package provides the FastAPI backend for the LijnDing Pipeline Framework's graphical user interface.

## Functionality

-   **Serves the Svelte Frontend**: Provides the main entry point for the web UI.
-   **Monitors Pipeline Runs**: Watches the `.lijnding_runs` directory for new pipeline execution logs.
-   **Exposes a REST API**: Provides endpoints for the frontend to query pipeline status, events, and logs.
-   **Provides Pipeline Control**: (Future) Endpoints to start, stop, or pause pipelines.

## Installation and Usage

1.  **Install the package**:
    ```bash
    pip install .
    ```

2.  **Run the server**:
    ```bash
    uvicorn lijnding_fastapi.main:app --reload
    ```

The server will be available at `http://127.0.0.1:8000`.
