#!/usr/bin/env python3
"""Unit tests for Silver layer cleaning logic."""
import unittest
from spark_jobs.silver.clean_trades import sanitize_column_name


class TestSilverClean(unittest.TestCase):
    """Test suite for Silver layer transformation helpers."""

    def test_sanitize_column_name(self):
        """Verify column name sanitization to snake_case."""
        cases = {
            "Dissemination Identifier": "dissemination_identifier",
            "Original Dissemination Identifier": "original_dissemination_identifier",
            "Action type": "action_type",
            "Event timestamp": "event_timestamp",
            "Notional amount-Leg 1": "notional_amount_leg_1",
            "Strike price currency/currency pair": "strike_price_currency_currency_pair",
            "Floating rate reset frequency period-leg 1": "floating_rate_reset_frequency_period_leg_1",
            "  Leading and Trailing Spaces  ": "leading_and_trailing_spaces",
            "_source_file": "source_file",
        }
        for raw, expected in cases.items():
            self.assertEqual(
                sanitize_column_name(raw),
                expected,
                f"Failed sanitizing '{raw}'",
            )


if __name__ == "__main__":
    unittest.main()
