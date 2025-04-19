# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DLT Medallion Framework Demo
# MAGIC 
# MAGIC This notebook demonstrates the functionality of the DLT Medallion Framework, including:
# MAGIC 1. Layer-specific decorators (Bronze, Silver, Gold)
# MAGIC 2. PII detection and masking
# MAGIC 3. Data quality expectations and metrics
# MAGIC 4. Metadata tracking
# MAGIC 5. Unity Catalog integration with column-level tags

# COMMAND ----------
# MAGIC %pip install dlt-framework

# COMMAND ----------

from datetime import datetime
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

from dlt_framework.decorators import bronze, silver, gold
from dlt_framework.core.dlt_integration import DLTIntegration

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Sample Data
# MAGIC First, let's create some sample data that includes PII fields

# COMMAND ----------

def generate_sample_data():
    """Generate sample customer data with PII fields."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("credit_card", StringType(), True),
        StructField("address", StringType(), True),
        StructField("purchase_amount", DoubleType(), True),
        StructField("transaction_time", TimestampType(), True)
    ])
    
    data = [
        ("C001", "john.doe@email.com", "+44 20 7123 4567", "4532-1234-5678-9012", "123 Main St, London", 150.50, datetime.now()),
        ("C002", "jane.smith@company.co.uk", "+1 415-555-0123", "5678-9012-3456-7890", "456 High St, Manchester", 75.25, datetime.now()),
        ("C003", "invalid.email", "12345", "not-a-card", "789 Park Ave", 200.00, datetime.now())
    ]
    
    return spark.createDataFrame(data, schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer: Raw Data Ingestion with PII Detection

# COMMAND ----------

@bronze(
    table={
        "name": "customer_data_bronze",
        "comment": "Raw customer transaction data with PII detection"
    },
    expectations=[
        {
            "name": "valid_customer_id",
            "constraint": "customer_id IS NOT NULL"
        },
        {
            "name": "valid_amount",
            "constraint": "purchase_amount > 0"
        }
    ],
    metrics=[
        {
            "name": "total_transactions",
            "value": "COUNT(*)"
        },
        {
            "name": "total_amount",
            "value": "SUM(purchase_amount)"
        }
    ],
    pii_detection=True  # Enable automatic PII detection
)
def ingest_customer_data():
    """Ingest raw customer data into bronze layer."""
    return generate_sample_data()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer: Data Cleansing with PII Masking

# COMMAND ----------

@silver(
    table={
        "name": "customer_data_silver",
        "comment": "Cleansed customer data with masked PII"
    },
    expectations=[
        {
            "name": "valid_email",
            "constraint": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
        }
    ],
    metrics=[
        {
            "name": "invalid_email_count",
            "value": "COUNT(CASE WHEN NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 1 END)"
        }
    ],
    masking_enabled=True,
    masking_overrides={
        "credit_card": "redact",  # Override default masking for credit cards
        "phone_number": "truncate"  # Override default masking for phone numbers
    }
)
def clean_customer_data():
    """Clean and mask customer data in silver layer."""
    df = dlt.read("customer_data_bronze")
    
    # Apply basic data cleansing
    return df.withColumn(
        "email",
        F.when(
            F.col("email").rlike('^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'),
            F.col("email")
        ).otherwise(None)
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer: Business-Level Aggregations

# COMMAND ----------

@gold(
    table={
        "name": "customer_metrics_gold",
        "comment": "Customer purchase metrics with PII verification"
    },
    expectations=[
        {
            "name": "positive_avg_amount",
            "constraint": "avg_purchase_amount > 0"
        }
    ],
    metrics=[
        {
            "name": "total_customers",
            "value": "COUNT(DISTINCT customer_id)"
        }
    ],
    verify_pii_masking=True  # Verify no unmasked PII exists
)
def calculate_customer_metrics():
    """Calculate customer-level metrics in gold layer."""
    df = dlt.read("customer_data_silver")
    
    return df.groupBy("customer_id").agg(
        F.avg("purchase_amount").alias("avg_purchase_amount"),
        F.count("*").alias("transaction_count"),
        F.max("transaction_time").alias("last_transaction")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Query the Results
# MAGIC After the pipeline runs, you can query the tables and examine the column-level tags

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Examine bronze layer data with PII detection results
# MAGIC SELECT * FROM LIVE.customer_data_bronze;
# MAGIC 
# MAGIC -- View silver layer data with masked PII
# MAGIC SELECT * FROM LIVE.customer_data_silver;
# MAGIC 
# MAGIC -- Check aggregated metrics in gold layer
# MAGIC SELECT * FROM LIVE.customer_metrics_gold;
# MAGIC 
# MAGIC -- View column-level tags for PII tracking
# MAGIC DESCRIBE EXTENDED LIVE.customer_data_silver; 