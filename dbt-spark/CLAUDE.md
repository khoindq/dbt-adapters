# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`dbt-spark` is a dbt adapter that enables dbt to work with Apache Spark. It supports multiple connection methods (Thrift, HTTP, ODBC, and session) and integrates with Databricks clusters and SQL endpoints.

## Development Commands

### Setup
```bash
# Install dependencies and pre-commit hooks
hatch run setup
```

### Testing

#### Unit Tests
```bash
# Run all unit tests
hatch run unit-tests

# Run a specific unit test file
python -m pytest tests/unit/test_adapter.py

# Run a specific test
python -m pytest tests/unit/test_adapter.py::TestSparkAdapter::test_acquire_connection
```

#### Integration Tests
Integration tests use [Dagger](https://dagger.io/) to orchestrate containers. Multiple profiles are supported:

- `apache_spark` - Apache Spark via Thrift
- `spark_session` - Spark session
- `spark_http_odbc` - Spark HTTP ODBC
- `databricks_sql_endpoint` - Databricks SQL endpoint
- `databricks_cluster` - Databricks cluster
- `databricks_http_cluster` - Databricks HTTP cluster

```bash
# Run all integration tests for a profile
hatch run integration-tests --profile apache_spark

# Run specific test module
hatch run integration-tests --profile apache_spark --test-path tests/functional/adapter/test_basic.py

# Run specific test
hatch run integration-tests --profile apache_spark --test-path tests/functional/adapter/test_basic.py::TestSimpleMaterializationsSpark::test_base
```

Note: For Databricks profiles, you'll need to set credentials in a `test.env` file (see `test.env.example`).

### Code Quality
```bash
# Run pre-commit checks (black, mypy, etc.)
hatch run code-quality
```

### Building
```bash
# Build package
hatch build

# Check wheel and sdist
hatch run build:check-all
```

### Local Development Environment

Start local Spark Thrift server with docker-compose:

```bash
# Start services
docker-compose up -d

# Complete reset (removes persisted data)
docker-compose down
rm -rf ./.hive-metastore/ ./.spark-warehouse/
```

Services:
- Spark UI: http://localhost:4040/sqlserver/
- Thrift endpoint: `jdbc:hive2://localhost:10000` (user: `dbt`, password: `dbt`)

## Architecture

### Connection Management

The adapter supports four connection methods via `SparkConnectionManager` (src/dbt/adapters/spark/connections.py):

1. **Thrift** - Uses PyHive to connect via Thrift protocol
2. **HTTP** - Uses PyHive with HTTP transport
3. **ODBC** - Uses pyodbc for ODBC connections (mainly for Databricks)
4. **Session** - Uses PySpark session for direct connections

Key connection classes:
- `SparkCredentials` - Stores connection parameters (host, port, method, token, etc.)
- `SparkConnectionManager` - Manages connection lifecycle
- `SparkConnectionWrapper` - Wraps different connection types with unified interface

### Adapter Implementation

`SparkAdapter` (src/dbt/adapters/spark/impl.py) extends `SQLAdapter` and implements Spark-specific functionality:

- Schema and relation metadata queries
- Catalog operations (SHOW commands, DESCRIBE TABLE)
- Python model support via `PythonJobHelper` implementations
- File format and storage options (parquet, delta, etc.)
- Partition and clustering configuration

### Python Submissions

For Python models, the adapter supports two execution modes (src/dbt/adapters/spark/python_submissions.py):

1. **JobClusterPythonJobHelper** - Creates ephemeral job clusters for each run
2. **AllPurposeClusterPythonJobHelper** - Uses existing all-purpose clusters

These integrate with Databricks Jobs API for execution.

### Macros and SQL

SQL macros are in `src/dbt/include/spark/macros/`:
- `adapters.sql` - Spark-specific implementations (LIST SCHEMAS, DESCRIBE TABLE EXTENDED, etc.)
- `materializations/` - Table, view, incremental, snapshot materializations
- `utils/` - Helper macros for Spark SQL generation

### Key Files

- `src/dbt/adapters/spark/impl.py` - Main adapter logic
- `src/dbt/adapters/spark/connections.py` - Connection management
- `src/dbt/adapters/spark/relation.py` - Relation type definitions
- `src/dbt/adapters/spark/column.py` - Column type handling
- `src/dbt/adapters/spark/session.py` - Spark session management
- `src/dbt/adapters/spark/python_submissions.py` - Python model execution

## Testing Architecture

- `tests/unit/` - Unit tests for adapter logic, connection handling, and macros
- `tests/functional/adapter/` - Functional tests that run against real Spark instances
- `dagger/run_dbt_spark_tests.py` - Orchestrates containerized test environments
- `tests/functional/conftest.py` - Pytest fixtures for functional tests

## Dependencies

This adapter depends on:
- `dbt-adapters` - Base adapter framework
- `dbt-common` - Common dbt utilities
- `dbt-core` - Core dbt functionality

Optional connection dependencies (installed via extras):
- PyHive + thrift - For Thrift/HTTP connections
- pyodbc - For ODBC connections
- pyspark - For session connections

## Important Notes

- The adapter uses Hatch for environment and task management
- CI environments use the `ci` hatch environment which has ddtrace enabled
- Integration tests require Dagger and container runtime (Docker/Podman)
- MacOS developers need `unixodbc` installed via Homebrew for ODBC support
