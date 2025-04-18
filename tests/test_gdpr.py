"""Tests for GDPR compliance functionality."""

import pytest
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType
from pyspark.sql import DataFrame

from dlt_framework.validation.gdpr import (
    GDPRField,
    GDPRValidator,
    create_gdpr_validator,
    PII_PATTERNS,
    RuleSet
)

@pytest.fixture
def sample_pii_fields():
    """Sample PII field configurations for testing."""
    return [
        {
            "name": "email",
            "pii_type": "email",
            "requires_consent": True,
            "retention_period": 365,
            "masking_strategy": "hash"
        },
        {
            "name": "phone",
            "pii_type": "phone_international",
            "requires_consent": True,
            "masking_strategy": "truncate"
        },
        {
            "name": "credit_card",
            "pii_type": "credit_card",
            "requires_consent": True,
            "masking_strategy": "redact"
        }
    ]

@pytest.fixture
def sample_data(spark_session):
    """Create a sample DataFrame with PII data."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("credit_card", StringType(), True),
        StructField("consent", BooleanType(), True)
    ])
    
    data = [
        ("1", "user1@example.com", "+1234567890", "4111111111111111", True),
        ("2", "user2@example.com", "+9876543210", "5555555555554444", False),
        ("3", "invalid_email", "12345", "invalid_card", None)
    ]
    
    return spark_session.createDataFrame(data, schema)

def test_gdpr_field_creation():
    """Test creation of GDPRField instances."""
    field = GDPRField(
        name="email",
        pii_type="email",
        requires_consent=True,
        retention_period=365,
        masking_strategy="hash"
    )
    
    assert field.name == "email"
    assert field.pii_type == "email"
    assert field.requires_consent is True
    assert field.retention_period == 365
    assert field.masking_strategy == "hash"

def test_validator_creation(sample_pii_fields):
    """Test creation of GDPRValidator from configuration."""
    validator = create_gdpr_validator(sample_pii_fields)
    
    assert len(validator.fields) == 3
    assert "email" in validator.fields
    assert "phone" in validator.fields
    assert "credit_card" in validator.fields
    
    assert len(validator.rules) == 3
    assert all(isinstance(rule_set, RuleSet) for rule_set in validator.rules.values())

def test_consent_validation(sample_data, sample_pii_fields):
    """Test validation of consent for PII fields."""
    validator = create_gdpr_validator(sample_pii_fields)
    result_df = validator.validate_consent(sample_data, "consent")
    
    # Check consent validation columns were added
    assert "email_has_consent" in result_df.columns
    assert "phone_has_consent" in result_df.columns
    assert "credit_card_has_consent" in result_df.columns
    
    # Verify consent validation results
    row1 = result_df.filter(F.col("id") == "1").collect()[0]
    assert row1["email_has_consent"] is True
    
    row2 = result_df.filter(F.col("id") == "2").collect()[0]
    assert row2["email_has_consent"] is False
    
    row3 = result_df.filter(F.col("id") == "3").collect()[0]
    assert row3["email_has_consent"] is False

def test_pii_masking(sample_data, sample_pii_fields):
    """Test masking of PII fields."""
    validator = create_gdpr_validator(sample_pii_fields)
    masked_df = validator.mask_pii(sample_data)
    
    # Check that masking was applied correctly
    row = masked_df.filter(F.col("id") == "1").collect()[0]
    
    # Email should be hashed
    assert row["email"] != "user1@example.com"
    assert len(row["email"]) == 64  # SHA-256 hash length
    
    # Phone should be truncated
    assert row["phone"] == "+123"
    
    # Credit card should be redacted
    assert row["credit_card"] == "REDACTED"

def test_pii_detection(sample_data, sample_pii_fields):
    """Test detection of potential PII fields."""
    validator = create_gdpr_validator(sample_pii_fields)
    detected_pii = validator.detect_pii(sample_data)
    
    assert "email" in detected_pii["email"]
    assert "phone" in detected_pii["phone_international"]
    assert "credit_card" in detected_pii["credit_card"]

def test_gdpr_metadata(sample_data, sample_pii_fields):
    """Test addition of GDPR metadata."""
    validator = create_gdpr_validator(sample_pii_fields)
    metadata_df = validator.add_gdpr_metadata(sample_data)
    
    # Check metadata columns were added
    assert "email_pii_type" in metadata_df.columns
    assert "phone_pii_type" in metadata_df.columns
    assert "credit_card_pii_type" in metadata_df.columns
    assert "email_retention_date" in metadata_df.columns
    
    # Verify metadata values
    row = metadata_df.filter(F.col("id") == "1").collect()[0]
    assert row["email_pii_type"] == "email"
    assert row["phone_pii_type"] == "phone_international"
    assert row["credit_card_pii_type"] == "credit_card"
    
    # Check retention date calculation
    retention_date = row["email_retention_date"]
    expected_date = datetime.now().date() + timedelta(days=365)
    assert retention_date == expected_date

def test_invalid_masking_strategy(sample_data):
    """Test handling of invalid masking strategy."""
    fields = [{
        "name": "email",
        "pii_type": "email",
        "masking_strategy": "invalid_strategy"
    }]
    
    validator = create_gdpr_validator(fields)
    result_df = validator.mask_pii(sample_data)
    
    # Should not modify the column if strategy is invalid
    row = result_df.filter(F.col("id") == "1").collect()[0]
    assert row["email"] == "user1@example.com"

def test_concurrent_validator_access(spark_session, sample_pii_fields):
    """Test concurrent access to GDPRValidator instance."""
    validator = create_gdpr_validator(sample_pii_fields)
    
    # Create a larger dataset for concurrent testing
    data = [(str(i), f"user{i}@example.com", f"+{i}" * 10, "4" * 16, bool(i % 2))
            for i in range(1000)]
    df = spark_session.createDataFrame(
        data,
        ["id", "email", "phone", "credit_card", "consent"]
    )
    
    results = []
    lock = threading.Lock()
    
    def concurrent_operation(op_type: str, df: DataFrame) -> None:
        """Execute concurrent operations on the validator."""
        try:
            if op_type == "mask":
                result_df = validator.mask_pii(df)
            elif op_type == "consent":
                result_df = validator.validate_consent(df, "consent")
            elif op_type == "detect":
                result = validator.detect_pii(df)
            elif op_type == "metadata":
                result_df = validator.add_gdpr_metadata(df)
                
            with lock:
                results.append((op_type, True))
        except Exception as e:
            with lock:
                results.append((op_type, False))
            raise e
    
    # Run multiple operations concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for op_type in ["mask", "consent", "detect", "metadata"] * 5:  # 20 total operations
            futures.append(executor.submit(concurrent_operation, op_type, df))
            
        # Wait for all operations to complete
        for future in as_completed(futures):
            future.result()  # This will raise any exceptions that occurred
    
    # Verify all operations completed successfully
    assert len(results) == 20
    assert all(success for _, success in results)
    
    # Verify validator state remains consistent
    assert len(validator.fields) == 3
    assert len(validator.rules) == 3

def test_retention_period_edge_cases(spark_session):
    """Test edge cases for retention period handling."""
    # Test immediate expiration
    fields = [{
        "name": "email",
        "pii_type": "email",
        "retention_period": 0
    }]
    validator = create_gdpr_validator(fields)
    
    # Create test data with timestamp
    data = [("1", "user1@example.com")]
    df = spark_session.createDataFrame(data, ["id", "email"])
    
    metadata_df = validator.add_gdpr_metadata(df)
    row = metadata_df.collect()[0]
    assert row["email_retention_date"] == datetime.now().date()
    
    # Test very long retention period
    fields = [{
        "name": "email",
        "pii_type": "email",
        "retention_period": 36500  # 100 years
    }]
    validator = create_gdpr_validator(fields)
    metadata_df = validator.add_gdpr_metadata(df)
    row = metadata_df.collect()[0]
    expected_date = datetime.now().date() + timedelta(days=36500)
    assert row["email_retention_date"] == expected_date
    
    # Test negative retention period (should default to None)
    fields = [{
        "name": "email",
        "pii_type": "email",
        "retention_period": -1
    }]
    validator = create_gdpr_validator(fields)
    metadata_df = validator.add_gdpr_metadata(df)
    assert "email_retention_date" not in metadata_df.columns

def test_performance_large_dataset(spark_session, sample_pii_fields):
    """Test performance with a large dataset."""
    validator = create_gdpr_validator(sample_pii_fields)
    
    # Create a large dataset (100K rows)
    num_rows = 100000
    data = [(str(i), f"user{i}@example.com", f"+{i}" * 10, "4" * 16, bool(i % 2))
            for i in range(num_rows)]
    df = spark_session.createDataFrame(
        data,
        ["id", "email", "phone", "credit_card", "consent"]
    )
    
    # Measure performance of each operation
    start_time = time.time()
    masked_df = validator.mask_pii(df)
    masking_time = time.time() - start_time
    
    start_time = time.time()
    consent_df = validator.validate_consent(df, "consent")
    consent_time = time.time() - start_time
    
    start_time = time.time()
    detected_pii = validator.detect_pii(df)
    detection_time = time.time() - start_time
    
    start_time = time.time()
    metadata_df = validator.add_gdpr_metadata(df)
    metadata_time = time.time() - start_time
    
    # Assert reasonable performance thresholds
    # Note: These thresholds might need adjustment based on the execution environment
    assert masking_time < 60  # Should complete within 60 seconds
    assert consent_time < 30
    assert detection_time < 120  # PII detection is more intensive
    assert metadata_time < 30
    
    # Verify results are correct
    assert masked_df.count() == num_rows
    assert consent_df.count() == num_rows
    assert len(detected_pii["email"]) > 0
    assert metadata_df.count() == num_rows

@pytest.fixture
def mock_dlt_context():
    """Mock DLT context for testing integration."""
    class MockDLTContext:
        def __init__(self):
            self.expectations = []
            self.metrics = []
        
        def expect(self, name, condition):
            self.expectations.append((name, condition))
            return True
        
        def add_metric(self, name, value):
            self.metrics.append((name, value))
    
    return MockDLTContext()

def test_dlt_integration(spark_session, sample_pii_fields, mock_dlt_context):
    """Test integration with DLT framework."""
    validator = create_gdpr_validator(sample_pii_fields)
    
    # Create test data
    data = [
        ("1", "user1@example.com", "+1234567890", "4111111111111111", True),
        ("2", "invalid_email", "12345", "invalid_card", False)
    ]
    df = spark_session.createDataFrame(
        data,
        ["id", "email", "phone", "credit_card", "consent"]
    )
    
    # Add DLT expectations for PII validation
    for field_name, field in validator.fields.items():
        if field.pii_type in PII_PATTERNS:
            condition = F.col(field_name).rlike(PII_PATTERNS[field.pii_type])
            mock_dlt_context.expect(
                f"valid_{field.pii_type}_{field_name}",
                condition
            )
    
    # Add DLT metrics for PII fields
    for field_name, field in validator.fields.items():
        if field.requires_consent:
            consent_count = df.filter(
                F.col("consent").isNotNull() & (F.col("consent") == True)
            ).count()
            mock_dlt_context.add_metric(
                f"{field_name}_consent_count",
                consent_count
            )
    
    # Verify DLT expectations and metrics were added
    assert len(mock_dlt_context.expectations) == 3  # One for each PII field
    assert len(mock_dlt_context.metrics) == 3  # One for each field requiring consent
    
    # Verify expectation conditions
    email_expectation = next(
        exp for exp in mock_dlt_context.expectations 
        if "email" in exp[0]
    )
    assert "valid_email" in email_expectation[0]
    
    # Verify metrics
    email_metric = next(
        metric for metric in mock_dlt_context.metrics 
        if "email" in metric[0]
    )
    assert email_metric[1] == 1  # Only one record has valid consent 