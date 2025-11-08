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
        """Test that relations with catalog render as catalog.schema.table"""
        relation = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table"
        )
        assert relation.render() == "`my_catalog`.`my_schema`.`my_table`"

    def test_relation_with_default_catalog(self):
        """Test that relations with 'default' catalog render as schema.table"""
        relation = SparkRelation.create(
            catalog="default",
            schema="my_schema",
            identifier="my_table"
        )
        assert relation.render() == "my_schema.my_table"

    def test_relation_with_default_catalog_case_insensitive(self):
        """Test that 'DEFAULT' catalog (uppercase) also renders as schema.table"""
        relation = SparkRelation.create(
            catalog="DEFAULT",
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
        # Should include catalog
        rel_with_catalog = SparkRelation.create(
            catalog="my_catalog",
            schema="my_schema",
            identifier="my_table"
        )
        assert rel_with_catalog.include_catalog() is True

        # Should not include default catalog
        rel_with_default = SparkRelation.create(
            catalog="default",
            schema="my_schema",
            identifier="my_table"
        )
        assert rel_with_default.include_catalog() is False

        # Should not include None catalog
        rel_with_none = SparkRelation.create(
            catalog=None,
            schema="my_schema",
            identifier="my_table"
        )
        assert rel_with_none.include_catalog() is False


if __name__ == "__main__":
    unittest.main()
