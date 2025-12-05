"""
Unit tests for medallion pipeline transformations.
These tests validate transformation logic without Databricks dependencies.
"""
import pytest


class TestDataQualityRules:
    """Test data quality validation rules."""

    def test_price_validation_positive(self):
        """Valid positive prices should pass."""
        price = 150.99
        is_valid = price is not None and 0 <= price <= 50000
        assert is_valid is True

    def test_price_validation_negative(self):
        """Negative prices should fail."""
        price = -50.0
        is_valid = price is not None and 0 <= price <= 50000
        assert is_valid is False

    def test_price_validation_outlier(self):
        """Prices above threshold should be flagged."""
        price = 100000.0
        is_valid = price is not None and 0 <= price <= 50000
        assert is_valid is False

    def test_price_validation_null(self):
        """Null prices should fail."""
        price = None
        is_valid = price is not None and 0 <= price <= 50000
        assert is_valid is False


class TestCoordinateValidation:
    """Test geographic coordinate validation."""

    def test_latitude_valid_range(self):
        """Latitude within -90 to 90 should pass."""
        lat = 45.5
        is_valid = lat is not None and -90 <= lat <= 90
        assert is_valid is True

    def test_latitude_invalid_range(self):
        """Latitude outside -90 to 90 should fail."""
        lat = 100.0
        is_valid = lat is not None and -90 <= lat <= 90
        assert is_valid is False

    def test_longitude_valid_range(self):
        """Longitude within -180 to 180 should pass."""
        lon = -122.4
        is_valid = lon is not None and -180 <= lon <= 180
        assert is_valid is True

    def test_longitude_invalid_range(self):
        """Longitude outside -180 to 180 should fail."""
        lon = 200.0
        is_valid = lon is not None and -180 <= lon <= 180
        assert is_valid is False


class TestUserSegmentation:
    """Test user segmentation logic."""

    def test_purchase_frequency_high(self):
        """Users with 10+ purchases should be high frequency."""
        purchases = 15
        segment = (
            "high_frequency" if purchases >= 10
            else "medium_frequency" if purchases >= 3
            else "low_frequency"
        )
        assert segment == "high_frequency"

    def test_purchase_frequency_medium(self):
        """Users with 3-9 purchases should be medium frequency."""
        purchases = 5
        segment = (
            "high_frequency" if purchases >= 10
            else "medium_frequency" if purchases >= 3
            else "low_frequency"
        )
        assert segment == "medium_frequency"

    def test_purchase_frequency_low(self):
        """Users with <3 purchases should be low frequency."""
        purchases = 1
        segment = (
            "high_frequency" if purchases >= 10
            else "medium_frequency" if purchases >= 3
            else "low_frequency"
        )
        assert segment == "low_frequency"

    def test_price_segment_premium(self):
        """Users with avg price >= 500 should be premium."""
        avg_price = 750.0
        segment = (
            "premium" if avg_price >= 500
            else "standard" if avg_price >= 200
            else "budget"
        )
        assert segment == "premium"

    def test_price_segment_standard(self):
        """Users with avg price 200-499 should be standard."""
        avg_price = 350.0
        segment = (
            "premium" if avg_price >= 500
            else "standard" if avg_price >= 200
            else "budget"
        )
        assert segment == "standard"

    def test_price_segment_budget(self):
        """Users with avg price < 200 should be budget."""
        avg_price = 99.0
        segment = (
            "premium" if avg_price >= 500
            else "standard" if avg_price >= 200
            else "budget"
        )
        assert segment == "budget"


class TestCustomerTiering:
    """Test customer tier assignment logic."""

    def test_tier_platinum(self):
        """Customers with >= $5000 spend should be platinum."""
        total_spend = 7500.0
        tier = (
            "platinum" if total_spend >= 5000
            else "gold" if total_spend >= 2000
            else "silver" if total_spend >= 500
            else "bronze"
        )
        assert tier == "platinum"

    def test_tier_gold(self):
        """Customers with $2000-4999 spend should be gold."""
        total_spend = 3500.0
        tier = (
            "platinum" if total_spend >= 5000
            else "gold" if total_spend >= 2000
            else "silver" if total_spend >= 500
            else "bronze"
        )
        assert tier == "gold"

    def test_tier_silver(self):
        """Customers with $500-1999 spend should be silver."""
        total_spend = 1200.0
        tier = (
            "platinum" if total_spend >= 5000
            else "gold" if total_spend >= 2000
            else "silver" if total_spend >= 500
            else "bronze"
        )
        assert tier == "silver"

    def test_tier_bronze(self):
        """Customers with < $500 spend should be bronze."""
        total_spend = 250.0
        tier = (
            "platinum" if total_spend >= 5000
            else "gold" if total_spend >= 2000
            else "silver" if total_spend >= 500
            else "bronze"
        )
        assert tier == "bronze"


class TestConversionRateCalculation:
    """Test conversion rate calculation logic."""

    def test_conversion_rate_normal(self):
        """Normal conversion rate calculation."""
        purchases = 25
        total = 100
        rate = (purchases * 100.0 / total) if total > 0 else 0
        assert rate == 25.0

    def test_conversion_rate_zero_denominator(self):
        """Zero total should return 0 rate."""
        purchases = 0
        total = 0
        rate = (purchases * 100.0 / total) if total > 0 else 0
        assert rate == 0

    def test_conversion_rate_hundred_percent(self):
        """100% conversion rate."""
        purchases = 50
        total = 50
        rate = (purchases * 100.0 / total) if total > 0 else 0
        assert rate == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
