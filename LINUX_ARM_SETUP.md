# Linux ARM64 Setup Guide (py-flow)

This guide covers the necessary steps to run `py-flow` on Linux ARM64 architectures (e.g., WSL on ARM64, Raspberry Pi, or ARM-based Cloud VMs). 

The platform has been enhanced with an **Architecture-Aware Compatibility Layer** that transparently handles the differences between x86_64 and aarch64 environments.

## 1. Prerequisites
Ensure you have the following installed on your ARM machine:
* **Python 3.12+** (recommended for best performance)
* **Java 21** (Required for QuestDB on ARM: `sudo apt install openjdk-21-jdk`)
* **uv** (high-performance Python package manager)
* **Docker** (for the Streaming Engine)
* **Standard Build Tools**: `gcc`, `make`, `python3-dev`

## 2. Standard Setup Workflow
The project recommends using `uv` for managed environments and faster installations.

### 2.1 Install uv
If you don't have `uv` installed:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2.2 Create Virtual Environment
Create a dedicated virtual environment. By default, `uv` uses `.venv`:
```bash
uv venv --python 3.12
source .venv/bin/activate
```
*Note: If your IDE or scripts expect `venv`, you can create it with `uv venv venv` or update your settings.*

Install the platform and all optional components in **editable mode** (`-e`):
```bash
uv pip install -e ".[all]"
```

### 2.4 Synchronize IDE (VS Code)
Ensure your IDE is pointing to the correct environment. In `.vscode/settings.json`, ensure the following is set:
```json
"python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python"
```
You can also select the interpreter manually by pressing `Ctrl+Shift+P` and typing `Python: Select Interpreter`.

## 3. Object Store (PostgreSQL) Support
The standard `pgserver` package frequently lacks pre-built wheels for Linux/ARM. The project's `pyproject.toml` has been updated with **Environment Markers** to automatically install `pixeltable-pgserver` as a native fallback on Linux/ARM systems.

### Architecture Compatibility (`pg_compat.py`)
The project now includes `store/pg_compat.py`. This module:
1. **Prioritizes default `pgserver`**: Ensures macOS (M1-M4) and x86 users are unaffected.
2. **Linux/ARM Fallback**: Dynamically loads `pixeltable-pgserver` if the standard package is missing on ARM64 Linux.
3. **Extension Shimming**: Automatically creates a SQL-based `uuid_generate_v4()` shim to satisfy `uuid-ossp` requirements without needing C-based native extensions.

## 4. Streaming Engine (Deephaven) Support
The `deephaven-server` Python package is currently x86_64 only. On ARM64 architectures, the Streaming Engine safely and transparently falls back to a **Docker Container** for full native performance.

### Automatic Docker Fallback
The updated `StreamingServer.start()` method is now architecture-aware:
* **Checks Port 10000**: It first pings the local port to see if a Docker container is already running.
* **Auto-Starts Container**: If no container is found, it automatically starts `ghcr.io/deephaven/server:latest` in detached mode as a background test fixture, connecting to it transparently.
* **Clean Shutdown**: When the server stops, the Docker container is automatically terminated, requiring zero manual management from developers.

## 5. Market Data (QuestDB) Support
QuestDB is used as the time-series backend for high-frequency tick data. The project previously defaulted to a bundled x86_64 Java Runtime (JRE), which crashes on ARM.

### Automatic Detection
The `QuestDBManager` in `timeseries/backends/questdb/manager.py` has been updated to:
1. **Identify ARM64 Linux**: Detects `aarch64/arm64` environments.
2. **Architecture-Independent Binary**: Automatically pulls the `no-jre` binary instead of the bundled x86 runtime.
3. **Native Java Integration**: Utilizes your system's native ARM64 Java to run the server as a high-performance JAR.
    *   **Tip**: Install with `sudo apt update && sudo apt install -y openjdk-21-jdk`.

## 6. Lakehouse Support
The `lakehouse` component (Data Lakehouse with Iceberg support) requires ICU capability for its Rust migration scripts. On ARM64 architectures, it seamlessly handles compatibility.

The platform's `LakehouseServer` leverages:
*   **PostgreSQL (Docker)**: Automatically falls back to a native `postgres:15` container in the background to ensure full ICU compatibility for Lakekeeper. 
*   **Lakekeeper**: The official Rust-based Iceberg REST catalog.
*   **MinIO**: High-performance object storage.
*   **DuckDB**: Integrated natively via standard Python wheels.

## 7. Python 3.10+ Timestamp Compatibility
PostgreSQL `NOTIFY` payloads often include timestamps with variable fractional precision (e.g., 5 digits). While older versions of Python were lenient, **Python 3.10+ is strict**.

The `store/subscriptions.py` module now includes a robust `_parse_iso` helper that normalizes these payloads to ensure the reactive event listener remains stable across all modern Python versions.

## 8. Verification
To verify your setup, run the integrated parallel test suite:
```bash
# Ensure your Deephaven container and local dependencies are available:
./run_all_tests.sh
```
