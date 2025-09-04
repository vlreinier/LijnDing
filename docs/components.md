# Components

LijnDing's functionality can be extended with optional components. These are installable packages that provide new pipeline stages, often for integrating with external systems like message queues or web services.

## Using Components

To use a component, you need to install its package. For example, to use the HTTP components, you would install `lijnding-http`:

```bash
pip install lijnding-http
```

Once installed, the component's stages will be available for import from `lijnding.components`.

```python
from lijnding.components.http import http_request

pipeline = Pipeline([http_request("https://api.example.com/data")])
```

## Creating Components

Creating a new component for LijnDing involves setting up a specific project structure that allows it to be installed as a namespace package. This guide walks you through the process.

### 1. Directory Structure

Your component should have its own directory within the `components/` directory of the main project. The structure should look like this:

```
components/
└── my_component/
    ├── pyproject.toml
    ├── README.md
    ├── src/
    │   └── lijnding/
    │       └── components/
    │           └── my_component.py
    └── tests/
        ├── __init__.py
        └── test_my_component.py
```

-   `components/my_component/`: The root directory for your component.
-   `pyproject.toml`: The packaging configuration for your component.
-   `src/lijnding/components/`: This nested structure is crucial for the namespace package to work. Your component's code goes in a `.py` file here.
-   `tests/`: This directory contains the tests for your component.

### 2. `pyproject.toml`

Your component's `pyproject.toml` file should be configured to make it an installable package that extends the `lijnding` namespace. Here is a template:

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "lijnding-my_component"
version = "0.1.0"
description = "My component for the LijnDing Pipeline Framework"
readme = "README.md"
requires-python = ">=3.8"
dependencies = [
    "lijnding-core",
    # Add any other dependencies your component needs
]

[project.optional-dependencies]
test = [
    "lijnding-core[test]",
    # Add any test-specific dependencies
]

[tool.setuptools.packages.find]
where = ["src"]
include = ["lijnding*"]
namespaces = true
```

**Key points:**

-   **`name`**: The package name should be prefixed with `lijnding-`.
-   **`dependencies`**: Your component must depend on `lijnding-core`.
-   **`[project.optional-dependencies.test]`**: Test-specific dependencies go here. Your component's tests should depend on `lijnding-core[test]`.
-   **`[tool.setuptools.packages.find]`**: The `where`, `include`, and `namespaces` settings are essential for the namespace package to be correctly discovered and installed.

### 3. Writing Tests

Tests for your component should be placed in the `tests/` directory. They will be discovered by `pytest` when running tests from the root of the LijnDing project.

To run all tests, including your new component's tests, you need to install all packages in editable mode from the root of the project:

```bash
pip install -e .[test-all] -e components/my_component
```

Then, you can run `pytest` from the root directory.
