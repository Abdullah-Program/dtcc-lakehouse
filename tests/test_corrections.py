#!/usr/bin/env python3
"""Unit tests for Trade Corrections Engine (Gold Layer)."""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from spark_jobs.gold.apply_corrections import (
    deduplicate_latest_batch,
    map_lifecycle_status_expr,
)


class TestTradeCorrections(unittest.TestCase):
    """Test suite for lifecycle mapping and batch deduplication."""

    @classmethod
    def setUpClass(cls):
        """Build minimal local SparkSession for unit testing."""
        cls.spark = (
            SparkSession.builder.appName("TestTradeCorrections")
            .master("local[1]")
            .config("spark.driver.memory", "500m")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Clean shutdown of test SparkSession."""
        cls.spark.stop()

    def test_lifecycle_status_mapping(self):
        """Verify regulatory action types map to correct lifecycle statuses."""
        schema = StructType([
            StructField("action_type", StringType(), False),
        ])
        data = [
            ("NEWT",),
            ("MODI",),
            ("CORR",),
            ("REVI",),
            ("TERM",),
            ("EROR",),
            ("OTHER",),
        ]
        df = self.spark.createDataFrame(data, schema=schema)
        df_result = df.withColumn("status", map_lifecycle_status_expr())
        mapping = {row["action_type"]: row["status"] for row in df_result.collect()}

        self.assertEqual(mapping["NEWT"], "ACTIVE")
        self.assertEqual(mapping["MODI"], "ACTIVE")
        self.assertEqual(mapping["CORR"], "ACTIVE")
        self.assertEqual(mapping["REVI"], "ACTIVE")
        self.assertEqual(mapping["TERM"], "TERMINATED")
        self.assertEqual(mapping["EROR"], "CANCELLED")
        self.assertEqual(mapping["OTHER"], "UNKNOWN")

    def test_deduplicate_latest_batch(self):
        """Verify that multiple mutations for a trade_key resolve to the latest record."""
        schema = StructType([
            StructField("trade_key", StringType(), False),
            StructField("dissemination_identifier", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("notional_amount_leg_1", DoubleType(), True),
        ])
        data = [
            # Trade 1: Root trade created, then modified, then corrected
            ("T1", "1001", "NEWT", 1000000.0),
            ("T1", "1002", "MODI", 1200000.0),
            ("T1", "1003", "CORR", 1250000.0),
            # Trade 2: Standalone trade
            ("T2", "2001", "NEWT", 5000000.0),
            # Trade 3: Trade created, then terminated
            ("T3", "3001", "NEWT", 3000000.0),
            ("T3", "3002", "TERM", 3000000.0),
        ]
        df = self.spark.createDataFrame(data, schema=schema)
        df_deduped = deduplicate_latest_batch(df)

        results = {row["trade_key"]: row for row in df_deduped.collect()}

        # Exactly 3 unique trades must remain
        self.assertEqual(len(results), 3)

        # Trade 1 must have the latest CORR state (id 1003)
        self.assertEqual(results["T1"]["dissemination_identifier"], "1003")
        self.assertEqual(results["T1"]["action_type"], "CORR")
        self.assertEqual(results["T1"]["lifecycle_status"], "ACTIVE")
        self.assertEqual(results["T1"]["notional_amount_leg_1"], 1250000.0)

        # Trade 2 must have NEWT state
        self.assertEqual(results["T2"]["dissemination_identifier"], "2001")
        self.assertEqual(results["T2"]["lifecycle_status"], "ACTIVE")

        # Trade 3 must have TERMINATED state (id 3002)
        self.assertEqual(results["T3"]["dissemination_identifier"], "3002")
        self.assertEqual(results["T3"]["action_type"], "TERM")
        self.assertEqual(results["T3"]["lifecycle_status"], "TERMINATED")


if __name__ == "__main__":
    unittest.main()
