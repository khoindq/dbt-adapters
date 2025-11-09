"""Tests for catalog support in dbt-spark"""
import unittest
from dbt.adapters.spark import SparkRelation


class TestCatalogSupport(unittest.TestCase):
    """Test catalog functionality in Spark relations"""

    def test_relation_without_catalog(self):
        """Test that relations without catalog render as schema.table"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table"
        )
        assert relation.render() == "my_schema.my_table"

    def test_relation_with_catalog(self):
        """Test that relations with catalog render as catalog.schema.table (no quotes by default)"""
        relation = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table"
        )
        assert relation.render() == "my_catalog.my_schema.my_table"

    def test_relation_with_empty_catalog(self):
        """Test that relations with empty string catalog render as schema.table"""
        relation = SparkRelation.create(
            catalog="",
            schema="my_schema",
            identifier="my_table"
        )
        assert relation.render() == "my_schema.my_table"

    def test_relation_with_whitespace_catalog(self):
        """Test that relations with whitespace-only catalog render as schema.table"""
        relation = SparkRelation.create(
            catalog="   ",
            schema="my_schema",
            identifier="my_table"
        )
        assert relation.render() == "my_schema.my_table"

    def test_relation_catalog_none(self):
        """Test that None catalog renders as schema.table"""
        relation = SparkRelation.create(
            catalog=None,
            schema="my_schema",
            identifier="my_table"
        )
        assert relation.render() == "my_schema.my_table"

    def test_include_catalog_method(self):
        """Test the include_catalog helper method"""
        # Should include non-empty catalog
        rel_with_catalog = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table"
        )
        assert rel_with_catalog.include_catalog() is True

        # Should not include empty catalog
        rel_with_empty = SparkRelation.create(
            catalog="",
            schema="my_schema",
            identifier="my_table"
        )
        assert rel_with_empty.include_catalog() is False

        # Should not include None catalog
        rel_with_none = SparkRelation.create(
            catalog=None,
            schema="my_schema",
            identifier="my_table"
        )
        assert rel_with_none.include_catalog() is False

        # Should not include whitespace-only catalog
        rel_with_whitespace = SparkRelation.create(
            catalog="   ",
            schema="my_schema",
            identifier="my_table"
        )
        assert rel_with_whitespace.include_catalog() is False


class TestCatalogQuoting(unittest.TestCase):
    """Test quoting behavior with catalog support"""

    def test_catalog_quoting_enabled(self):
        """Test that catalog is quoted when quote_policy.catalog is True"""
        from dbt.adapters.spark.relation import SparkQuotePolicy

        quote_policy = SparkQuotePolicy(catalog=True, schema=False, identifier=False)
        relation = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table",
            quote_policy=quote_policy
        )
        assert relation.render() == "`my_catalog`.my_schema.my_table"

    def test_all_components_quoted(self):
        """Test that all components are quoted when all quote policies are True"""
        from dbt.adapters.spark.relation import SparkQuotePolicy

        quote_policy = SparkQuotePolicy(catalog=True, schema=True, identifier=True)
        relation = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table",
            quote_policy=quote_policy
        )
        assert relation.render() == "`my_catalog`.`my_schema`.`my_table`"

    def test_catalog_and_identifier_quoted(self):
        """Test selective quoting of catalog and identifier only"""
        from dbt.adapters.spark.relation import SparkQuotePolicy

        quote_policy = SparkQuotePolicy(catalog=True, schema=False, identifier=True)
        relation = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table",
            quote_policy=quote_policy
        )
        assert relation.render() == "`my_catalog`.my_schema.`my_table`"

    def test_no_quoting_by_default(self):
        """Test that default quote policy does not quote any components"""
        relation = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table"
        )
        # Default quote policy has all False
        assert relation.render() == "my_catalog.my_schema.my_table"

    def test_quoting_without_catalog(self):
        """Test that quoting works correctly for 2-level namespace"""
        from dbt.adapters.spark.relation import SparkQuotePolicy

        quote_policy = SparkQuotePolicy(schema=True, identifier=True)
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            quote_policy=quote_policy
        )
        assert relation.render() == "`my_schema`.`my_table`"

    def test_catalog_quote_policy_independent(self):
        """Test that catalog quote policy is independent from database quote policy"""
        from dbt.adapters.spark.relation import SparkQuotePolicy

        # Even if database is True, catalog should only be quoted if catalog is True
        quote_policy = SparkQuotePolicy(database=True, catalog=False, schema=False, identifier=False)
        relation = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table",
            quote_policy=quote_policy
        )
        # Catalog should NOT be quoted (catalog=False)
        assert relation.render() == "my_catalog.my_schema.my_table"

        # Now test with catalog=True
        quote_policy2 = SparkQuotePolicy(database=False, catalog=True, schema=False, identifier=False)
        relation2 = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table",
            quote_policy=quote_policy2
        )
        # Catalog SHOULD be quoted (catalog=True)
        assert relation2.render() == "`my_catalog`.my_schema.my_table"


if __name__ == "__main__":
    unittest.main()
