# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Medallion Framework Validation
# MAGIC 
# MAGIC This notebook tests the functionality of the DLT Medallion Framework within Databricks.
# MAGIC 
# MAGIC ## Features Tested:
# MAGIC 1. Layer decorators (Bronze, Silver, Gold)
# MAGIC 2. Schema validation
# MAGIC 3. Basic validation rules
# MAGIC 4. GDPR/PII functionality
# MAGIC 5. Quarantine management
# MAGIC 6. DLT integration
# MAGIC 
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install /dbfs/FileStore/jars/dlt_framework-0.1.0-py3-none-any.whl

# COMMAND ----------

from datetime import datetime
from typing import Dict, List

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import pyspark.sql.functions as F

from dlt_framework.decorators import bronze, silver, gold
from dlt_framework.validation import SchemaValidator
from dlt_framework.validation.rules import NumericRangeRule, RegexRule
from dlt_framework.validation.gdpr import GDPRValidator, PIIField
from dlt_framework.validation.quarantine import QuarantineConfig, QuarantineManager

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Sample Data

# COMMAND ----------

def generate_sample_data() -> DataFrame:
    """Generate sample customer data with PII fields."""
    data = [
        (1, "john.doe@email.com", "+1-555-0123", "4111-1111-1111-1111", 100.50, datetime.now(), True),
        (2, "invalid_email", "12345", "invalid_card", 50.25, datetime.now(), False),
        (3, "jane.smith@email.com", "+44-20-7123-4567", "5555-5555-5555-5555", 75.00, datetime.now(), True),
        (4, None, None, None, -10.00, datetime.now(), None)
    ]
    
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("credit_card", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_time", TimestampType(), True),
        StructField("consent_given", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze Layer Pipeline

# COMMAND ----------

@dlt.table
@bronze(
    expectations=[
        {"name": "valid_customer_id", "constraint": "customer_id IS NOT NULL"},
        {"name": "valid_amount", "constraint": "amount >= 0"}
    ],
    metrics=[
        {"name": "total_transactions", "value": "COUNT(*)"},
        {"name": "total_amount", "value": "SUM(amount)"}
    ],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def raw_transactions() -> DataFrame:
    """Ingest raw transaction data."""
    df = generate_sample_data()
    
    # Configure quarantine for invalid records
    quarantine_config = QuarantineConfig(
        source_table_name="LIVE.raw_transactions",  # Use the DLT table name
        error_column="_error",
        timestamp_column="_quarantine_timestamp",
        batch_id_column="_batch_id",
        source_column="_source"
    )
    
    quarantine_manager = QuarantineManager(quarantine_config)
    
    # Set up schema validation
    expected_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("credit_card", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_time", TimestampType(), True),
        StructField("consent_given", StringType(), True)
    ])
    
    schema_validator = SchemaValidator(
        expected_schema=expected_schema,
        evolution_mode="additive",
        quarantine_path="/tmp/quarantine/schema_violations"
    )
    
    # Validate schema
    validation_result = schema_validator.validate(df)
    
    # Quarantine invalid records
    if validation_result.invalid_records is not None:
        # Prepare records for quarantine
        quarantine_df = quarantine_manager.prepare_quarantine_records(
            validation_result.invalid_records,
            error_details="Schema validation failure",
            batch_id=dlt.current_pipeline_run_id()
        )
        # Write to the quarantine stream
        dlt.write(quarantine_df, "raw_transactions_quarantine_records")
    
    return validation_result.valid_records

# COMMAND ----------

# Create the quarantine table for raw_transactions
@dlt.table(
    name="raw_transactions_quarantine",
    comment="Quarantine table for invalid raw transaction records"
)
def create_raw_transactions_quarantine():
    return QuarantineManager.create_quarantine_table_for("LIVE.raw_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Layer Pipeline

# COMMAND ----------

@dlt.table
@silver(
    expectations=[
        {"name": "valid_email", "constraint": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"},
        {"name": "consent_status", "constraint": "consent_given IS NOT NULL"}
    ],
    metrics=[
        {"name": "valid_email_count", "value": "COUNT(*) FILTER (WHERE email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')"},
        {"name": "consent_given_count", "value": "COUNT(*) FILTER (WHERE consent_given = true)"}
    ]
)
def cleaned_transactions() -> DataFrame:
    """Clean and validate transaction data."""
    df = dlt.read("raw_transactions")
    
    # Set up GDPR validation
    pii_fields = {
        "email": PIIField(pii_type="email", requires_consent=True),
        "phone": PIIField(pii_type="phone", requires_consent=True),
        "credit_card": PIIField(pii_type="credit_card", requires_consent=True)
    }
    
    gdpr_validator = GDPRValidator(pii_fields)
    
    # Add validation rules
    rules = [
        NumericRangeRule(min_value=0, name="valid_amount"),
        RegexRule(pattern="^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$", name="valid_email")
    ]
    
    # Apply GDPR validation and masking
    df = gdpr_validator.validate_and_mask(df, consent_column="consent_given")
    
    # Apply validation rules
    for rule in rules:
        df = df.withColumn(
            f"{rule.name}_check",
            rule.validate(F.col(rule.name.replace("valid_", "")))
        )
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Layer Pipeline

# COMMAND ----------

@dlt.table
@gold(
    expectations=[
        {"name": "all_amounts_valid", "constraint": "amount > 0"},
        {"name": "all_emails_masked", "constraint": "email LIKE '%*****%'"}
    ],
    metrics=[
        {"name": "total_customers", "value": "COUNT(DISTINCT customer_id)"},
        {"name": "avg_amount", "value": "AVG(amount)"}
    ]
)
def customer_metrics() -> DataFrame:
    """Calculate customer-level metrics."""
    df = dlt.read("cleaned_transactions")
    
    return df.groupBy("customer_id").agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.first("email").alias("email"),
        F.first("phone").alias("phone"),
        F.first("credit_card").alias("credit_card")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execute Pipeline and View Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View bronze layer data
# MAGIC SELECT * FROM LIVE.raw_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View silver layer data with masked PII
# MAGIC SELECT * FROM LIVE.cleaned_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View gold layer aggregated metrics
# MAGIC SELECT * FROM LIVE.customer_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View quarantined records
# MAGIC SELECT * FROM LIVE.raw_transactions_quarantine;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Examine DLT Pipeline Metrics and Expectations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View DLT metrics
# MAGIC SELECT * FROM system.raw_transactions.metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View DLT expectations
# MAGIC SELECT * FROM system.raw_transactions.expectations; 