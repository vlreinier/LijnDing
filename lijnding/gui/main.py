import asyncio
import json
from pathlib import Path
from typing import List, Dict, Any

import aiofiles
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.staticfiles import StaticFiles # Will be used later

# Placeholder for security features
# from fastapi import Depends, Security
# from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI()

# --- Security Setup (Placeholders) ---
# security = HTTPBasic() # Example: Basic Auth
# TODO: Implement JWT, CSRF protection, etc.

# --- CORS Middleware ---
# This is necessary to allow the Svelte frontend (on a different port)
# to communicate with the FastAPI backend.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Svelte's default dev port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Application State ---
# In a real app, this might be a more robust service class.
BASE_RUNS_DIR = Path(".lijnding_runs")


# --- API Endpoints ---

@app.get("/api/runs")
async def get_all_runs() -> List[Dict[str, Any]]:
    """
    Scans the base log directory and returns a summary of all pipeline runs.
    """
    if not BASE_RUNS_DIR.exists():
        return []

    runs = []
    for run_dir in BASE_RUNS_DIR.iterdir():
        if run_dir.is_dir():
            # In a more robust implementation, we'd read a summary file.
            # For now, we just return the run ID.
            runs.append({"run_id": run_dir.name})

    # Sort by directory name (which is a UUID, not ideal for time sorting)
    runs.sort(key=lambda x: x["run_id"], reverse=True)
    return runs


@app.get("/api/runs/{run_id}")
async def get_run_details(run_id: str) -> Dict[str, Any]:
    """
    Returns the detailed events for a specific pipeline run.
    """
    run_dir = BASE_RUNS_DIR / run_id
    log_file = run_dir / "events.log"

    if not log_file.exists():
        raise HTTPException(status_code=404, detail="Run not found or log file is missing.")

    events = []
    try:
        async with aiofiles.open(log_file, "r") as f:
            async for line in f:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    # Ignore lines that are not valid JSON
                    pass
        return {"run_id": run_id, "events": events}
    except IOError as e:
        raise HTTPException(status_code=500, detail=f"Error reading log file: {e}")


# --- Static File Serving (Placeholder) ---
# This will be used to serve the built Svelte application.
# app.mount("/", StaticFiles(directory="path/to/svelte/dist", html=True), name="static")

@app.get("/")
async def root():
    """
    Root endpoint, useful for health checks.
    Later, this will serve the Svelte app's index.html.
    """
    return {"message": "LijnDing FastAPI backend is running."}
