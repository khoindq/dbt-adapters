# dbt-spark Feature Plans

This directory contains detailed feature plans and architectural designs for dbt-spark enhancements.

## Current Plans

### 1. information_schema Migration

**File**: `information_schema_migration.md`

**Summary**: Migrate from `SHOW TABLE EXTENDED` to `information_schema.tables` for listing relations.

**Key Changes**:
- Update `spark__list_relations_without_caching()` macro to query `information_schema.tables`
- Modify Python parsing logic in `SparkAdapter._get_relation_information()`
- Add fallback to legacy `SHOW TABLE EXTENDED` for older Spark versions
- Maintain backward compatibility with existing information field format

**Impact**:
- ✅ Improves cache population performance (30-50% faster in large schemas)
- ✅ Aligns with Databricks best practices
- ✅ Provides richer metadata access
- ✅ No breaking changes - automatic fallback for compatibility

**Caching Note**:
Despite the method name `list_relations_without_caching()`, this method IS used BY dbt-core TO POPULATE its cache. The changes will automatically improve dbt's cache population phase without requiring any modifications to cache-related methods like `cache_added()`, `cache_dropped()`, or `get_relation()`.

## References

- [Databricks information_schema.tables](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html)
- [dbt Adapter Cache Documentation](https://docs.getdbt.com/reference/global-configs/cache)
- [dbt list_relations_without_caching Discussion](https://github.com/dbt-labs/dbt-spark/issues/228)
