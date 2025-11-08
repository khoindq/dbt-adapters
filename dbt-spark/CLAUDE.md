# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

This is the `dbt-spark` adapter, located in a subdirectory of the parent `dbt-adapters` monorepo. This adapter enables dbt to work with Apache Spark, including support for multiple connection methods (Thrift, HTTP, ODBC, Session) and various Spark distributions (Apache Spark, Databricks clusters, Databricks SQL endpoints).

**Important**: This is a subdirectory within a larger git repository. All git operations should be performed with awareness that the root is at `/Users/mini/workspace/dbt-adapters/`, but development work focuses on the `dbt-spark` subdirectory.

### Catalog Support (3-Level Namespace)

dbt-spark now supports 3-level namespace with catalog support (Spark 3.4+). When a catalog is specified in the profile, relations will be rendered as `catalog.schema.table`. When catalog is omitted or set to "default", the traditional 2-level namespace `schema.table` is used for backward compatibility.

## Development Setup

```bash
# Initial setup (installs pre-commit hooks and dagger dependencies)
hatch run setup

# Run code quality checks (pre-commit on all files)
hatch run code-quality
```

## Testing Commands

### Unit Tests
```bash
# Run all unit tests
hatch run unit-tests

# Run specific unit test file
hatch run unit-tests tests/unit/test_connections.py

# Run specific test
hatch run unit-tests tests/unit/test_connections.py::TestConnection::test_method
```

### Integration Tests

Integration tests use [dagger](https://dagger.io/) to orchestrate test containers. Multiple test profiles are supported:

**Available profiles**:
- `apache_spark` - Apache Spark via Thrift
- `spark_session` - Spark session (pyspark)
- `spark_http_odbc` - ODBC connection
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

### Local Development with Docker

For local testing against Apache Spark:

```bash
# Start Spark Thrift server and Postgres (Hive Metastore)
docker-compose up -d

# Complete reset of local environment
docker-compose down
rm -rf ./.hive-metastore/
rm -rf ./.spark-warehouse/
```

The local Spark instance provides:
- Spark UI at http://localhost:4040/sqlserver/
- Thrift endpoint at `jdbc:hive2://localhost:10000` (credentials: `dbt:dbt`)

## Architecture Overview

### Core Components

**Connection Management** (`connections.py`):
- Defines `SparkCredentials` dataclass with all connection parameters
- Implements `SparkConnectionManager` with multiple connection wrapper classes:
  - `ThriftConnectionWrapper` - For Thrift protocol
  - `HttpConnectionWrapper` - For HTTP protocol
  - `OdbcConnectionWrapper` - For ODBC drivers
  - `SessionConnectionWrapper` - For pyspark sessions
- Handles connection retries, timeouts, and query polling
- Query retry logic for connection losses during long-running queries

**Adapter Implementation** (`impl.py`):
- `SparkAdapter` extends `SQLAdapter`
- Key methods for Spark-specific operations:
  - `list_relations_without_caching()` - Uses SHOW TABLES or metadata queries
  - `get_columns_in_relation()` - Retrieves column metadata
  - `parse_describe_extended()` - Parses DESCRIBE EXTENDED output
- Handles catalog/schema naming (Spark treats database and schema as equivalent)
- Python model support via `SparkPythonJobHelper`

**Relation Types** (`relation.py`):
- `SparkRelation` extends `BaseRelation`
- Handles table/view/CTE relation types

**Column Handling** (`column.py`):
- `SparkColumn` with Spark-specific data type handling
- Type conversion and validation

**Python Submissions** (`python_submissions.py`):
- `JobClusterPythonJobHelper` - For Databricks job clusters
- `AllPurposeClusterPythonJobHelper` - For all-purpose clusters
- Handles Python model execution on Spark/Databricks

### Macros and Materializations

SQL macros are located in `src/dbt/include/spark/macros/`:

**Materializations**:
- `incremental/` - Incremental model strategies (merge, insert_overwrite, append)
- `table.sql` - Table materialization
- `view.sql` - View materialization
- `snapshot.sql` - Snapshot functionality
- `seed.sql` - Seed data handling
- `clone.sql` - Clone operations

**Adapters**:
- Schema and catalog operations
- Relation listing and metadata queries
- Column operations and constraints

### Key Spark-Specific Concepts

**Catalog Support (3-Level Namespace)**:
- Spark 3.4+ introduces catalog support for 3-level namespace: `catalog.schema.table`
- Configure `catalog` in profile credentials to use 3-level naming
- When `catalog` is omitted or set to `"default"` (case-insensitive), uses 2-level naming for backward compatibility
- `SparkRelation.include_catalog()` determines if catalog should be included in rendered SQL
- Relations automatically render with catalog when specified: `catalog.schema.table`
- **Usage in dbt models**: When catalog is configured, you can reference tables with `{{ ref('model_name') }}` and it will automatically use the 3-level namespace

**Database vs Schema**: On Spark, `database` and `schema` are synonymous. The adapter enforces that if both are specified, they must match. Internally, the adapter uses `schema` and sets `database` to `None`.

**Connection Methods**: Four distinct connection methods with different dependency requirements:
- `thrift`: Requires PyHive, thrift, and related packages
- `http`: Similar to thrift but uses HTTP transport
- `odbc`: Requires pyodbc and ODBC driver installation
- `session`: Requires pyspark for in-process Spark sessions

**File Formats**: Supports various file formats (parquet, delta, orc, etc.) configured via `SparkConfig.file_format`

**Incremental Strategies**:
- `merge` - Using MERGE INTO (requires Delta/Iceberg)
- `insert_overwrite` - Static or dynamic partition overwrite
- `append` - Simple INSERT INTO

## Dependencies

**Core dependencies** (from `pyproject.toml`):
- `dbt-common>=1.10,<2.0`
- `dbt-adapters>=1.7,<2.0`
- `dbt-core>=1.8.0`
- `sqlparams>=3.0.0`

**Optional dependencies**:
- `[ODBC]`: pyodbc for ODBC connections
- `[PyHive]`: PyHive and thrift for Thrift/HTTP connections
- `[session]`: pyspark for session-based connections
- `[all]`: All optional dependencies

## Build and Release

```bash
# Build wheel and source distribution
hatch build

# Check built packages
hatch run build:check-all
```

## Project Structure Notes

- Source code: `src/dbt/adapters/spark/` - Main adapter implementation
- Macros: `src/dbt/include/spark/macros/` - SQL macros and materializations
- Tests: `tests/unit/` and `tests/functional/`
- Dagger config: `dagger/` - Integration test orchestration
- Docker config: `docker/` - Local development containers

## Configuration Examples

### Profile with Catalog Support

To use 3-level namespace with catalog support (Spark 3.4+):

```yaml
spark_testing:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: 127.0.0.1
      port: 10000
      user: dbt
      schema: analytics
      catalog: my_catalog  # Add this for 3-level namespace support
```

When `catalog` is configured:
- Tables render as: `my_catalog.analytics.my_table`
- `ref()` and `source()` functions automatically include catalog
- Set `catalog: default` or omit to use 2-level namespace

## Common Issues

**Database/Schema confusion**: Remember that Spark treats database and schema as the same. Always use `schema` in profiles and ensure `database` is either omitted or matches `schema`.

**Catalog configuration**: The `catalog` field is optional. When omitted or set to "default" (case-insensitive), dbt-spark uses 2-level namespace (`schema.table`) for backward compatibility. Only specify a catalog name when you need 3-level namespace support (Spark 3.4+).

**Connection dependencies**: If encountering import errors, ensure the appropriate optional dependencies are installed (`pip install dbt-spark[ODBC]`, etc.)

**Query timeouts**: For long-running queries, configure `query_timeout` and `poll_interval` in connection credentials. The adapter polls async queries and can retry on connection loss.

**Hive Metastore**: Local development requires a Hive Metastore backend (provided by docker-compose Postgres container).
