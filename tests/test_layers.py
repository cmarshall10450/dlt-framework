"""Test layer-specific decorators functionality."""
from typing import Dict, List, Protocol
import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

from dlt_framework.core.config_models import (
    BronzeConfig,
    SilverConfig,
    GoldConfig,
    Expectation,
    Metric,
)
from dlt_framework.decorators import bronze, silver, gold
from dlt_framework.core.registry import DecoratorRegistry


class MockPIIDetector(Protocol):
    """Mock PII detector for testing."""
    def detect_pii(self, df: DataFrame) -> Dict[str, List[str]]:
        """Mock PII detection."""
        ...


class MockPIIMasker(Protocol):
    """Mock PII masker for testing."""
    def mask_pii(self, df: DataFrame) -> DataFrame:
        """Mock PII masking."""
        ...


@pytest.fixture
def mock_pii_detector():
    """Create a mock PII detector."""
    detector = Mock(spec=MockPIIDetector)
    detector.detect_pii.return_value = {"email": ["email"], "phone": []}
    return detector


@pytest.fixture
def mock_pii_masker():
    """Create a mock PII masker."""
    masker = Mock(spec=MockPIIMasker)
    masker.mask_pii.side_effect = lambda df: df  # Return input DataFrame unchanged
    return masker


@pytest.fixture(autouse=True)
def clear_registry():
    """Clear the decorator registry before each test."""
    DecoratorRegistry().clear()


def test_bronze_decorator(spark_session, mock_dlt, mock_pii_detector):
    """Test bronze layer decorator functionality."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True)
        ])
        data = [(1, "test@example.com"), (2, "test2@example.com")]
        df = spark_session.createDataFrame(data, schema)
        
        # Create configuration
        config = BronzeConfig(
            validate=[
                Expectation(name="valid_id", constraint="id IS NOT NULL"),
                Expectation(name="valid_email", constraint="email IS NOT NULL")
            ],
            metrics=[
                Metric(name="null_id_count", value="COUNT(CASE WHEN id IS NULL THEN 1 END)")
            ],
            pii_detection=True
        )
        
        @bronze(config=config, pii_detector=mock_pii_detector)
        def process_bronze(df):
            return df
        
        # Test decorated function
        result_df = process_bronze(df)
        assert result_df.count() == df.count()
        
        # Verify PII detection was called
        mock_pii_detector.detect_pii.assert_called_once()
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(f"bronze_{process_bronze.__name__}")
        assert metadata["layer"] == "bronze"


def test_silver_decorator(spark_session, mock_dlt, mock_pii_masker):
    """Test silver layer decorator functionality."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True)
        ])
        data = [(1, "test@example.com"), (2, "invalid")]
        df = spark_session.createDataFrame(data, schema)
        
        # Create configuration
        config = SilverConfig(
            validate=[
                Expectation(
                    name="valid_email",
                    constraint="email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
                )
            ],
            metrics=[
                Metric(
                    name="invalid_email_count",
                    value="COUNT(CASE WHEN NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 1 END)"
                )
            ],
            masking_enabled=True,
            masking_overrides={"email": "hash"}
        )
        
        @silver(config=config, pii_masker=mock_pii_masker)
        def process_silver(df):
            return df
        
        # Test decorated function
        result_df = process_silver(df)
        assert result_df.count() == df.count()
        
        # Verify PII masking was called
        mock_pii_masker.mask_pii.assert_called_once()
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(f"silver_{process_silver.__name__}")
        assert metadata["layer"] == "silver"


def test_gold_decorator(spark_session, mock_dlt, mock_pii_detector):
    """Test gold layer decorator functionality."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("amount", DoubleType(), True),
            StructField("email", StringType(), True)
        ])
        data = [(None, 100.0, "test@example.com"), (None, 200.0, "test2@example.com")]
        df = spark_session.createDataFrame(data, schema)
        
        # Create configuration
        config = GoldConfig(
            validate=[
                Expectation(name="positive_amount", constraint="amount > 0")
            ],
            metrics=[
                Metric(name="total_amount", value="SUM(amount)")
            ],
            verify_pii_masking=True
        )
        
        # Configure mock to detect no PII (as expected in gold layer)
        mock_pii_detector.detect_pii.return_value = {"email": [], "phone": []}
        
        @gold(config=config, pii_detector=mock_pii_detector)
        def process_gold(df):
            return df.groupBy("date").sum("amount")
        
        # Test decorated function
        result_df = process_gold(df)
        assert result_df.count() == 1  # Grouped by date (all NULL)
        
        # Verify PII detection was called
        mock_pii_detector.detect_pii.assert_called_once()
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(f"gold_{process_gold.__name__}")
        assert metadata["layer"] == "gold"


def test_gold_decorator_pii_error(spark_session, mock_dlt, mock_pii_detector):
    """Test gold layer decorator PII detection error."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame with unmasked PII
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("amount", DoubleType(), True),
            StructField("email", StringType(), True)
        ])
        data = [(None, 100.0, "test@example.com"), (None, 200.0, "test2@example.com")]
        df = spark_session.createDataFrame(data, schema)
        
        # Create configuration
        config = GoldConfig(
            validate=[
                Expectation(name="positive_amount", constraint="amount > 0")
            ],
            metrics=[
                Metric(name="total_amount", value="SUM(amount)")
            ],
            verify_pii_masking=True
        )
        
        # Configure mock to detect unmasked PII
        mock_pii_detector.detect_pii.return_value = {"email": ["email"], "phone": []}
        
        @gold(config=config, pii_detector=mock_pii_detector)
        def process_gold(df):
            return df
        
        # Test that unmasked PII raises an error
        with pytest.raises(ValueError, match="Detected unmasked PII data in gold layer"):
            process_gold(df)


def test_decorator_stacking(spark_session, mock_dlt, mock_pii_detector):
    """Test stacking layer decorators with other decorators."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True)
        ])
        data = [(1, "test@example.com"), (2, "test2@example.com")]
        df = spark_session.createDataFrame(data, schema)
        
        # Create configuration
        config = BronzeConfig(
            validate=[
                Expectation(name="valid_id", constraint="id IS NOT NULL")
            ],
            metrics=[
                Metric(name="row_count", value="COUNT(*)")
            ],
            pii_detection=True
        )
        
        @bronze(config=config, pii_detector=mock_pii_detector)
        def process_data(df):
            return df
        
        # Test decorated function
        result_df = process_data(df)
        assert result_df.count() == df.count()
        
        # Verify PII detection was called
        mock_pii_detector.detect_pii.assert_called_once()
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(f"bronze_{process_data.__name__}")
        assert metadata["layer"] == "bronze" 