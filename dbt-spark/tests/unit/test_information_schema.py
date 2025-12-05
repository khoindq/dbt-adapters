"""Tests for information_schema.tables migration"""
import unittest
import pytest
from multiprocessing import get_context

from dbt.adapters.spark import SparkAdapter
from tests.unit.utils import config_from_parts_or_dicts


class TestInformationSchemaIntegration(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def set_up_fixtures(self, target_http, base_project_cfg):
        self.base_project_cfg = base_project_cfg
        self.target_http = target_http

    def test_build_information_string_from_information_schema(self):
        """Test building information string from information_schema columns"""
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        # Test with delta table
        info_string = adapter._build_information_string(
            table_type="MANAGED",
            data_source_format="DELTA",
            table_owner="test_user",
            storage_path="s3://bucket/path",
            comment="Test table",
        )

        assert "Type: MANAGED" in info_string
        assert "Provider: delta" in info_string
        assert "Owner: test_user" in info_string
        assert "Location: s3://bucket/path" in info_string
        assert "Comment: Test table" in info_string

        # Test with VIEW
        info_string = adapter._build_information_string(
            table_type="VIEW",
            data_source_format="",
            table_owner="test_user",
            storage_path="",
            comment="Test view",
        )

        assert "Type: VIEW" in info_string
        assert "Owner: test_user" in info_string
        assert "Comment: Test view" in info_string

    def test_get_relation_information_from_information_schema(self):
        """Test parsing row from information_schema.tables"""
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        # Mock row from information_schema.tables
        row_data = {
            "table_catalog": "main",
            "table_schema": "test_schema",
            "table_name": "test_table",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "table_owner": "test_user",
            "comment": "Test table",
            "storage_path": "s3://bucket/path",
        }

        # Create mock row with keys() method
        class MockRow:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data.get(key)

            def keys(self):
                return self._data.keys()

        mock_row = MockRow(row_data)

        schema, name, information = adapter._get_relation_information(mock_row)

        assert schema == "test_schema"
        assert name == "test_table"
        assert "Type: MANAGED" in information
        assert "Provider: delta" in information
        assert "Owner: test_user" in information

    def test_build_spark_relation_list_case_insensitive(self):
        """Test that provider detection is case insensitive"""
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        class MockRow:
            def __init__(self, schema, name, info):
                self._schema = schema
                self._name = name
                self._info = info

        def mock_relation_info(row):
            return row._schema, row._name, row._info

        # Test with lowercase provider
        rows = [
            MockRow("test_schema", "delta_table", "Type: MANAGED\nProvider: delta"),
            MockRow("test_schema", "hudi_table", "Type: MANAGED\nProvider: hudi"),
            MockRow("test_schema", "iceberg_table", "Type: MANAGED\nProvider: iceberg"),
        ]

        relations = adapter._build_spark_relation_list(rows, mock_relation_info)

        assert len(relations) == 3
        assert relations[0].is_delta
        assert relations[1].is_hudi
        assert relations[2].is_iceberg

    def test_get_relation_information_legacy(self):
        """Test legacy SHOW TABLE EXTENDED parsing"""
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        # Mock row from SHOW TABLE EXTENDED
        row_data = ["test_schema", "test_table", False, "Type: MANAGED\nProvider: delta"]

        class MockRow:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, index):
                return self._data[index]

            def __iter__(self):
                return iter(self._data)

        mock_row = MockRow(row_data)

        schema, name, information = adapter._get_relation_information_legacy(mock_row)

        assert schema == "test_schema"
        assert name == "test_table"
        assert "Type: MANAGED" in information
        assert "Provider: delta" in information

    def test_information_string_with_empty_values(self):
        """Test building information string with empty/None values"""
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        info_string = adapter._build_information_string(
            table_type="",
            data_source_format="",
            table_owner="",
            storage_path="",
            comment="",
        )

        # Should still have Type: MANAGED as default
        assert "Type: MANAGED" in info_string

    def test_information_string_with_iceberg(self):
        """Test building information string for iceberg tables"""
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        info_string = adapter._build_information_string(
            table_type="EXTERNAL",
            data_source_format="ICEBERG",
            table_owner="iceberg_user",
            storage_path="s3://iceberg/warehouse",
            comment="Iceberg table",
        )

        assert "Type: MANAGED" in info_string  # Non-VIEW becomes MANAGED
        assert "Provider: iceberg" in info_string
        assert "Owner: iceberg_user" in info_string
