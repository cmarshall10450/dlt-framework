"""Tests for schema evolution functionality in the DLT Medallion Framework.

This module tests the schema evolution capabilities of the SchemaValidator class,
including different evolution modes (strict, additive, all) and various schema
change scenarios.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)

from dlt_framework.validation.schema import SchemaValidator, ValidationResult

@pytest.fixture
def spark_session():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .appName("schema_evolution_test")
            .master("local[1]")
            .getOrCreate())

@pytest.fixture
def base_schema():
    """Create a base schema for testing."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True)
    ])

@pytest.fixture
def sample_data(spark_session):
    """Create sample data matching the base schema."""
    data = [
        ("1", "John Doe", 30, "john@example.com"),
        ("2", "Jane Smith", 25, "jane@example.com")
    ]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True)
    ])
    return spark_session.createDataFrame(data, schema)

def test_strict_mode_rejects_new_columns(spark_session, base_schema):
    """Test that strict mode rejects schemas with new columns."""
    # Create validator in strict mode
    validator = SchemaValidator(base_schema, evolution_mode="strict")
    
    # Create data with an extra column
    data = [("1", "John", 30, "john@example.com", "extra")]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("extra_col", StringType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)
    
    # Validate schema
    result = validator.validate(df)
    assert not result.valid_records.schema.fieldNames() == df.schema.fieldNames()
    assert "new fields not allowed" in str(result.error_counts).lower()

def test_additive_mode_accepts_new_columns(spark_session, base_schema):
    """Test that additive mode accepts new columns but rejects type changes."""
    # Create validator in additive mode
    validator = SchemaValidator(base_schema, evolution_mode="additive")
    
    # Create data with an extra column
    data = [("1", "John", 30, "john@example.com", "extra")]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("extra_col", StringType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)
    
    # Validate schema
    result = validator.validate(df)
    assert "extra_col" in result.valid_records.schema.fieldNames()
    assert len(result.valid_records.schema.fields) == len(df.schema.fields)

def test_additive_mode_rejects_type_changes(spark_session, base_schema):
    """Test that additive mode rejects type changes."""
    validator = SchemaValidator(base_schema, evolution_mode="additive")
    
    # Create data with a type change (age: Int -> String)
    data = [("1", "John", "30", "john@example.com")]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),  # Changed from IntegerType
        StructField("email", StringType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)
    
    # Validate schema
    result = validator.validate(df)
    assert "type mismatch" in str(result.error_counts).lower()

def test_all_mode_accepts_type_changes(spark_session, base_schema):
    """Test that 'all' mode accepts both new columns and type changes."""
    validator = SchemaValidator(base_schema, evolution_mode="all")
    
    # Create data with both new columns and type changes
    data = [("1", "John", "30", "john@example.com", True)]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),  # Changed type
        StructField("email", StringType(), True),
        StructField("is_active", BooleanType(), True)  # New column
    ])
    df = spark_session.createDataFrame(data, schema)
    
    # Validate schema
    result = validator.validate(df)
    assert len(result.valid_records.schema.fields) == len(df.schema.fields)
    assert "is_active" in result.valid_records.schema.fieldNames()
    assert result.valid_records.schema["age"].dataType == StringType()

def test_missing_required_columns(spark_session, base_schema):
    """Test that missing required columns are rejected in all modes."""
    validator = SchemaValidator(base_schema, evolution_mode="all")
    
    # Create data missing the required 'id' column
    data = [("John", 30, "john@example.com")]
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)
    
    # Validate schema
    result = validator.validate(df)
    assert "missing required field" in str(result.error_counts).lower()

def test_metrics_tracking(spark_session, base_schema, sample_data):
    """Test that validation metrics are tracked correctly."""
    validator = SchemaValidator(base_schema, evolution_mode="strict", track_metrics=True)
    
    # Add some invalid records
    data = [
        ("1", "John", 30, "john@example.com"),  # Valid
        ("2", "Jane", "25", "jane@example.com"),  # Invalid type
        ("3", None, 35, "bob@example.com")  # Valid
    ]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),  # Wrong type
        StructField("email", StringType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)
    
    # Validate and check metrics
    result = validator.validate(df)
    assert result.metrics is not None
    assert result.metrics["total_records"] == 3
    assert result.metrics["validity_rate"] < 1.0
    assert result.metrics["invalid_records"] > 0

def test_quarantine_functionality(spark_session, base_schema, tmp_path):
    """Test that invalid records are properly quarantined."""
    quarantine_path = str(tmp_path / "quarantine")
    validator = SchemaValidator(
        base_schema,
        evolution_mode="strict",
        quarantine_path=quarantine_path
    )
    
    # Create data with some invalid records
    data = [
        ("1", "John", 30, "john@example.com"),  # Valid
        ("2", "Jane", "25", "jane@example.com"),  # Invalid type
        ("3", "Bob", 35, "bob@example.com", "extra")  # Extra column
    ]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),  # Wrong type
        StructField("email", StringType(), True),
        StructField("extra", StringType(), True)  # Extra column
    ])
    df = spark_session.createDataFrame(data, schema)
    
    # Validate and check quarantine
    result = validator.validate(df)
    
    # Check quarantined records
    quarantined = spark_session.read.format("delta").load(quarantine_path)
    assert quarantined.count() > 0
    assert "_error_details" in quarantined.columns
    assert "_quarantine_timestamp" in quarantined.columns 