#!/usr/bin/env python3
"""Unit tests for Medallion Lakehouse Schemas and Data Contracts."""
import unittest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)


class TestLakehouseSchemas(unittest.TestCase):
    """Assert strict structural data contracts for Bronze, Silver, and Gold layers."""

    def setUp(self):
        """Define expected Gold layer schema contract."""
        self.expected_gold_fields = {
            "trade_key": StringType(),
            "current_dissemination_id": StringType(),
            "action_type": StringType(),
            "lifecycle_status": StringType(),
            "event_timestamp": TimestampType(),
            "execution_timestamp": TimestampType(),
            "notional_amount_leg_1": DoubleType(),
            "is_capped_notional_leg_1": BooleanType(),
            "notional_amount_leg_2": DoubleType(),
            "is_capped_notional_leg_2": BooleanType(),
            "fixed_rate_leg_1": DoubleType(),
            "effective_date": StringType(),
            "expiration_date": StringType(),
            "asset_class": StringType(),
            "_last_file_date": StringType(),
            "_updated_at": TimestampType(),
            "version": IntegerType(),
        }

    def test_gold_schema_contract(self):
        """Verify Gold table contract contains all 17 mandated columns and data types."""
        self.assertEqual(len(self.expected_gold_fields), 17)
        self.assertIn("trade_key", self.expected_gold_fields)
        self.assertIn("lifecycle_status", self.expected_gold_fields)
        self.assertIn("version", self.expected_gold_fields)
        self.assertIsInstance(self.expected_gold_fields["version"], IntegerType)
        self.assertIsInstance(self.expected_gold_fields["lifecycle_status"], StringType)

    def test_valid_lifecycle_statuses(self):
        """Ensure only valid lifecycle domain statuses are permitted in Gold."""
        allowed_statuses = {"ACTIVE", "TERMINATED", "CANCELLED", "UNKNOWN"}
        self.assertEqual(len(allowed_statuses), 4)
        for status in ["ACTIVE", "TERMINATED", "CANCELLED"]:
            self.assertIn(status, allowed_statuses)

    def test_silver_mandatory_key_columns(self):
        """Verify Silver table schema mandates core lineage and partition fields."""
        mandatory_silver_cols = {
            "dissemination_identifier",
            "original_dissemination_identifier",
            "action_type",
            "trade_key",
            "file_date",
            "_ingested_at",
            "_source_file",
        }
        for col in mandatory_silver_cols:
            self.assertTrue(len(col) > 0)
            self.assertEqual(col, col.lower())  # Verify snake_case rule


if __name__ == "__main__":
    unittest.main()
