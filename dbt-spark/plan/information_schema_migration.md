# Feature Plan: Migrate dbt-spark from SHOW TABLE EXTENDED to information_schema.tables

## Overview

Migrate dbt-spark's table listing mechanism from Spark SQL's `SHOW TABLE EXTENDED` command to the standardized `information_schema.tables` view. This aligns with Databricks' modern metadata access patterns and provides richer metadata.

### About Relation Caching

The `list_relations_without_caching()` method is the core method that queries the database to discover relations. Despite its name, this method is used BY dbt-core TO POPULATE its internal cache at the start of a run. The "without_caching" refers to the fact that this method doesn't use dbt's cache - it always queries the database directly.

dbt-core's caching mechanism:
1. At the start of a run, dbt calls `list_relations_without_caching()` to populate its cache
2. During the run, dbt uses the cache to quickly check relation existence
3. When relations are created/dropped/renamed via adapter methods, dbt automatically updates the cache

Therefore, updating `list_relations_without_caching()` to use information_schema will automatically improve the performance of dbt's cache population phase.

## Current Implementation

### Command Used
```sql
SHOW TABLE EXTENDED IN <schema> LIKE '*'
```

### Output Structure
Returns 4 columns:
- `database` (schema name)
- `tableName`
- `isTemporary` (boolean)
- `information` (string containing metadata blob)

### Information Field Contents
The `information` field is a text blob containing key-value pairs like:
```
Database: my_schema
Table: my_table
Owner: user@company.com
Created Time: ...
Last Access: ...
Created By: ...
Type: MANAGED/EXTERNAL/VIEW
Provider: delta/parquet/hudi/iceberg
Location: s3://...
```

### Parsing Logic Location
- **Macro**: `src/dbt/include/spark/macros/adapters.sql:295-301` - `spark__list_relations_without_caching()`
- **Python**: `src/dbt/adapters/spark/impl.py:232-270` - `list_relations_without_caching()`
- **Helper**: `src/dbt/adapters/spark/impl.py:163-172` - `_get_relation_information()`
- **Builder**: `src/dbt/adapters/spark/impl.py:200-230` - `_build_spark_relation_list()`

### Key Metadata Extracted
From the `information` blob, the code extracts:
- Relation type (VIEW vs TABLE) - by checking "Type: VIEW"
- Delta tables - by checking "Provider: delta"
- Hudi tables - by checking "Provider: hudi"
- Iceberg tables - by checking "Provider: iceberg"

## Target Implementation

### New Query
```sql
SELECT * FROM information_schema.tables
WHERE table_schema = '<schema>'
```

### information_schema.tables Structure

Based on [Databricks documentation](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html):

| Column | Type | Description |
|--------|------|-------------|
| `table_catalog` | STRING | Catalog containing the relation |
| `table_schema` | STRING | Schema containing the relation |
| `table_name` | STRING | Name of the relation |
| `table_type` | STRING | Type: VIEW, MANAGED, EXTERNAL, STREAMING_TABLE, MATERIALIZED_VIEW, FOREIGN, MANAGED_SHALLOW_CLONE |
| `is_insertable_into` | STRING | "YES" or "NO" |
| `commit_action` | STRING | Always "PRESERVE" |
| `table_owner` | STRING | Owner principal |
| `comment` | STRING | Optional comment/description |
| `created` | TIMESTAMP | Creation timestamp |
| `created_by` | STRING | Creator principal |
| `last_altered` | TIMESTAMP | Last modification timestamp |
| `last_altered_by` | STRING | Last modifier principal |
| `data_source_format` | STRING | Format: PARQUET, DELTA, CSV, ORC, etc. |
| `storage_path` | STRING | Storage location URL |
| `storage_sub_directory` | STRING | Always NULL (discontinued) |

## Implementation Plan

### 1. Update SQL Macro (adapters.sql)

**File**: `src/dbt/include/spark/macros/adapters.sql`

**Changes to `spark__list_relations_without_caching()`** (lines 295-301):

```sql
{% macro spark__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog,
      table_schema,
      table_name,
      table_type,
      data_source_format,
      table_owner,
      comment,
      storage_path
    from information_schema.tables
    where table_schema = '{{ relation.schema }}'
  {% endcall %}

  {% do return(load_result('list_relations_without_caching').table) %}
{% endmacro %}
```

**Note**: We select specific columns instead of `*` to ensure consistent column ordering and avoid breaking changes.

### 2. Update Python Parsing Logic (impl.py)

**File**: `src/dbt/adapters/spark/impl.py`

**A. Update `_get_relation_information()` method** (lines 163-172):

Change from parsing 4-column SHOW TABLE EXTENDED output to parsing information_schema output:

```python
def _get_relation_information(self, row: "agate.Row") -> RelationInfo:
    """Relation info fetched from information_schema.tables"""
    try:
        table_catalog = row["table_catalog"]
        table_schema = row["table_schema"]
        table_name = row["table_name"]
        table_type = row["table_type"]
        data_source_format = row["data_source_format"] or ""
        table_owner = row["table_owner"] or ""
        comment = row["comment"] or ""
        storage_path = row["storage_path"] or ""
    except (KeyError, ValueError) as e:
        raise DbtRuntimeError(
            f'Invalid value from information_schema.tables: {e}'
        )

    # Build information string compatible with existing parsing logic
    information = self._build_information_string(
        table_type=table_type,
        data_source_format=data_source_format,
        table_owner=table_owner,
        storage_path=storage_path,
        comment=comment
    )

    return table_schema, table_name, information
```

**B. Add new helper method `_build_information_string()`**:

```python
def _build_information_string(
    self,
    table_type: str,
    data_source_format: str,
    table_owner: str,
    storage_path: str,
    comment: str
) -> str:
    """
    Build information string from information_schema columns
    to maintain compatibility with existing parsing logic.
    """
    info_parts = []

    # Map information_schema table_type to legacy format
    if table_type == "VIEW":
        info_parts.append("Type: VIEW")
    else:
        info_parts.append("Type: MANAGED")

    # Add provider information from data_source_format
    if data_source_format:
        provider = data_source_format.lower()
        info_parts.append(f"Provider: {provider}")

    if table_owner:
        info_parts.append(f"Owner: {table_owner}")

    if storage_path:
        info_parts.append(f"Location: {storage_path}")

    if comment:
        info_parts.append(f"Comment: {comment}")

    return "\n".join(info_parts)
```

**C. Update `_build_spark_relation_list()` method** (lines 200-230):

Update the provider detection logic to handle both lowercase and uppercase formats:

```python
def _build_spark_relation_list(
    self,
    row_list: "agate.Table",
    relation_info_func: Callable[["agate.Row"], RelationInfo],
) -> List[BaseRelation]:
    """Aggregate relations with format metadata included."""
    relations = []
    for row in row_list:
        _schema, name, information = relation_info_func(row)

        rel_type: RelationType = (
            RelationType.View
            if "Type: VIEW" in information
            else RelationType.Table
        )

        # Support both legacy format and new format
        information_lower = information.lower()
        is_delta: bool = ("provider: delta" in information_lower or
                         "data_source_format: delta" in information_lower)
        is_hudi: bool = ("provider: hudi" in information_lower or
                        "data_source_format: hudi" in information_lower)
        is_iceberg: bool = ("provider: iceberg" in information_lower or
                           "data_source_format: iceberg" in information_lower)

        relation: BaseRelation = self.Relation.create(
            schema=_schema,
            identifier=name,
            type=rel_type,
            information=information,
            is_delta=is_delta,
            is_iceberg=is_iceberg,
            is_hudi=is_hudi,
        )
        relations.append(relation)

    return relations
```

### 3. Handle Compatibility and Fallback

**Update `list_relations_without_caching()` method** (lines 232-270):

Add fallback logic for older Spark/Databricks versions that may not support information_schema:

```python
def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
    """Fetch relation list using information_schema, with fallback to legacy commands."""

    kwargs = {"schema_relation": schema_relation}

    try:
        # Try information_schema approach first
        info_schema_rows = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        return self._build_spark_relation_list(
            row_list=info_schema_rows,
            relation_info_func=self._get_relation_information,
        )
    except DbtRuntimeError as e:
        errmsg = getattr(e, "msg", "")

        # Handle schema not found
        if f"Database '{schema_relation}' not found" in errmsg or \
           f"Schema '{schema_relation.schema}' not found" in errmsg:
            return []

        # Fallback to legacy SHOW TABLE EXTENDED if information_schema not available
        if "SCHEMA_NOT_FOUND" in errmsg or "information_schema" in errmsg.lower():
            logger.debug(
                f"information_schema not available, falling back to SHOW TABLE EXTENDED"
            )
            return self._list_relations_using_show_table_extended(schema_relation)

        # Handle Iceberg v2 tables
        if "SHOW TABLE EXTENDED is not supported for v2 tables" in errmsg:
            return self._list_relations_using_show_tables(schema_relation)

        logger.debug(f"Error while retrieving relations in {schema_relation}: {errmsg}")
        return []

def _list_relations_using_show_table_extended(self, schema_relation: BaseRelation) -> List[BaseRelation]:
    """Legacy fallback using SHOW TABLE EXTENDED"""
    # Keep existing implementation as fallback
    kwargs = {"schema_relation": schema_relation}
    show_table_extended_rows = self.execute_macro(
        "list_relations_without_caching_legacy",
        kwargs=kwargs
    )
    return self._build_spark_relation_list(
        row_list=show_table_extended_rows,
        relation_info_func=self._get_relation_information_legacy,
    )
```

### 4. Create Legacy Fallback Macro

**File**: `src/dbt/include/spark/macros/adapters.sql`

Add a new macro for fallback:

```sql
{% macro spark__list_relations_without_caching_legacy(relation) %}
  {#-- Legacy fallback for environments without information_schema --#}
  {% call statement('list_relations_without_caching_legacy', fetch_result=True) -%}
    show table extended in {{ relation.schema }} like '*'
  {% endcall %}

  {% do return(load_result('list_relations_without_caching_legacy').table) %}
{% endmacro %}
```

### 5. Cache Compatibility

Since `list_relations_without_caching()` is used by dbt-core to populate its relation cache, we must ensure the output format remains compatible:

**Cache Requirements:**
1. Method must return `List[BaseRelation]` with correct schema, identifier, and type
2. Relations must have `is_delta`, `is_iceberg`, `is_hudi` flags set correctly
3. The `information` field must be populated for downstream compatibility

**No changes needed for:**
- `cache_added()` - Called when dbt creates a relation
- `cache_dropped()` - Called when dbt drops a relation
- `cache_renamed()` - Called when dbt renames a relation
- `get_relation()` - Uses cache first, then falls back to database query

These methods work at a higher level and aren't affected by how we populate the initial cache.

### 6. Testing Requirements

#### Unit Tests
**File**: `tests/unit/test_adapter.py`

Add tests for:
1. Parsing information_schema.tables output
2. Building information string from information_schema columns
3. Detecting delta/hudi/iceberg formats from data_source_format
4. Fallback behavior when information_schema is unavailable
5. Verify cache population works correctly with information_schema results
6. Ensure BaseRelation objects have all required fields populated

#### Integration Tests
**File**: `tests/functional/adapter/test_basic.py`

Test scenarios:
1. List relations using information_schema (Databricks, modern Spark)
2. Verify delta/hudi/iceberg detection still works
3. Verify VIEW vs TABLE detection
4. Verify fallback to SHOW TABLE EXTENDED on older Spark versions
5. Test with Apache Spark profile (which may not have information_schema)
6. Test with Databricks profiles (which should use information_schema)
7. Test dbt operations that rely on cache (incremental models, ref(), etc.)
8. Test with schemas containing 100+ tables to verify performance improvement

### 7. Performance Benchmarking

Before and after the change, measure:
1. Time to execute `list_relations_without_caching()` in a schema with 10, 100, 1000 tables
2. dbt startup time (cache population phase)
3. Memory usage for parsing results

Expected improvements:
- 30-50% faster cache population in large schemas (100+ tables)
- Lower memory overhead due to structured data vs text parsing

### 8. Documentation Updates

**File**: `README.md`

Add note about:
- information_schema support requirement
- Minimum Spark/Databricks versions that support information_schema
- Automatic fallback behavior for older versions

## Benefits

1. **Standardization**: Uses SQL standard information_schema instead of Spark-specific SHOW commands
2. **Richer Metadata**: Access to additional metadata (timestamps, storage paths, etc.)
3. **Better Performance**: information_schema views are optimized by the metastore, which improves cache population speed
4. **Improved Cache Population**: Faster initial cache population means faster dbt startup, especially in schemas with many tables
5. **Databricks Alignment**: Matches Databricks best practices and Unity Catalog patterns
6. **Consistency**: Same approach can be used across different Spark distributions

## Compatibility Considerations

### Version Support
- **Databricks Runtime 9.0+**: Full information_schema support
- **Apache Spark 3.3+**: Basic information_schema support (may vary by metastore)
- **Older versions**: Automatic fallback to SHOW TABLE EXTENDED

### Breaking Changes
None expected - the information field format is maintained for backward compatibility.

## Rollout Strategy

1. **Phase 1**: Implement with fallback (this plan)
2. **Phase 2**: Add configuration option to force information_schema or legacy mode
3. **Phase 3**: (Future) Deprecate legacy SHOW TABLE EXTENDED approach

## Alternative Approaches Considered

1. **Direct migration without fallback**: Rejected due to compatibility concerns
2. **Configuration flag to choose mode**: Deferred to Phase 2 to keep initial implementation simple
3. **Dual query (both methods)**: Rejected due to performance overhead

## References

- [Databricks information_schema.tables Documentation](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html)
- [Databricks Information Schema Overview](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema)
- [Apache Spark SHOW TABLE EXTENDED](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-tables.html)
