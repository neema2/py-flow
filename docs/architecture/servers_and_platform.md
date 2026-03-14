# Platform Servers & Infrastructure Services

This document details the core architectural pattern for all major backend services in `py-flow`, explaining how they are engineered to provide a zero-config, "batteries-included" experience for developers, particularly on macOS.

## 1. The Unified Server Pattern

`py-flow` is designed around a strictly decoupled Admin vs. User architectural pattern. Every infrastructure component follows the exact same lifecycle interface, hiding complex deployment topologies (PostgreSQL, Deephaven JVM, MinIO) from the user code.

The universal lifecycle consists of:
1. **Admin Instantiation**: Constructing an `XxxServer` instance (e.g., `StoreServer(data_dir="...")`).
2. **Lifecycle Control**: Calling `.start()` and `.stop()` to spin up or tear down the underlying binary dependencies.
3. **Alias Registration**: Calling `.register_alias("demo")` to map the complex connection details (ports, credentials, directories) to a simple string alias.
4. **User Access**: User code connects exclusively via the alias (e.g., `connect("demo")`, `Lakehouse("demo")`).

### Summary of Key Servers

| Server Component | Package Location | Underlying Infrastructure | User Client API |
|------------------|------------------|---------------------------|-----------------|
| **StoreServer** | `store.admin` | Embedded PostgreSQL + Row-Level Security | `connect("alias")` |
| **WorkflowServer**| `workflow.admin` | Embedded PostgreSQL + DBOS Transact | `create_engine("alias")` |
| **StreamingServer**| `streaming.admin` | Deephaven JVM Engine | `StreamingClient()` |
| **MarketDataServer**| `marketdata.admin` | FastAPI + Python Simulator + QuestDB | REST / WebSocket |
| **TsdbServer** | `timeseries.admin` | QuestDB Native Binary | `Timeseries("alias")` |
| **MediaServer** | `media.admin` | S3 Object Store (MinIO) | `MediaStore("alias")` |
| **LakehouseServer**| `lakehouse.admin` | Lakekeeper (Iceberg Catalog) + S3 + PG | `Lakehouse("alias")` |
| **SchedulerServer**| `scheduler.admin` | Embedded PG + DBOS + croniter | `Scheduler("alias")` |

## 2. Infrastructure Execution (macOS / x86_64)

The platform goes to great lengths to ensure that **macOS users** and standard x86_64 Linux users experience a fully native, Docker-free execution environment. The dependencies resolve transparently during a `pip install` without requiring external system services.

### Embedded PostgreSQL (`StoreServer`, `WorkflowServer`, `SchedulerServer`)
- **Mechanism**: Relies on the `pgserver` Python package.
- **MacOS Behavior**: `pgserver` ships with highly optimized, pre-built PostgreSQL binaries packaged directly into Python wheels. When `.start()` is called, `py-flow` spawns a localized PostgreSQL process bound to a temporary or project-local `.pgdata` directory.
- **Benefits**: No `brew install postgresql` required. No port conflicts. Full support for native PG extensions like `pgvector`.

### Streaming Engine (`StreamingServer`)
- **Mechanism**: Relies on the `deephaven-server` Python library.
- **MacOS Behavior**: Deephaven natively bootstraps its internally embedded JVM directly inside the running Python process using `jpy`. It binds to localhost to serve its browser UI and gRPC endpoints.
- **Benefits**: Zero container overhead. The live ticking tables share memory with the Python host. (Note: On ARM64 Linux systems where `deephaven-server` wheels are unavailable, the platform dynamically falls back to a background Docker container).

### Time-Series Database (`TsdbServer`, `MarketDataServer`)
- **Mechanism**: Relies on QuestDB binaries.
- **MacOS Behavior**: Automatically downloads the official QuestDB bundled runtime (combining the database and JRE) the first time it is invoked, caching it locally. It runs as an independent child process managed by the Python server wrapper.

### Lakehouse & Media (MinIO / Lakekeeper)
- **Mechanism**: Depending on the specific server, it utilizes lightweight background threads or downloads pre-compiled native binaries (like `minio` or `lakekeeper`) to simulate enterprise Cloud capabilities locally.

## 3. Advantages of the Architecture

By enforcing this strict localized server paradigm:
- **Seamless Testing**: The test suite can blindly spin up 1,000+ tests by initiating ephemeral servers on randomized ports, executing interactions, and tearing them down, preventing test state bleed.
- **Portability**: Code written against `connect("demo")` behaves identically on a developer's Macbook (talking to `pgserver`) as it would in production (where "demo" might map to an AWS RDS Aurora instance).
- **Reduced Friction**: New developers only need `pip install -e .` and `python` to run complex multi-node topologies (PostgreSQL, QuestDB, Deephaven, Iceberg).
