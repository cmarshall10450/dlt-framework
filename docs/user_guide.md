# DLT Medallion Framework User Guide

## Overview

The DLT Medallion Framework is a Python library that provides a structured approach to building data pipelines using the medallion architecture pattern with Delta Lake tables. It combines data quality checks, metrics tracking, and metadata management through a simple decorator-based API.

## Key Features

- Layer-specific decorators (`@bronze`, `@silver`, `@gold`)
- Data quality expectations and metrics
- Configuration templates for common patterns
- Delta Lake integration
- Metadata tracking and lineage

## Installation

```bash
pip install dlt-framework
```

## Quick Start

Here's a simple example of using the framework:

```python
from dlt_framework.decorators import bronze
from pyspark.sql import DataFrame

@bronze(
    expectations=[
        {
            "name": "valid_id",
            "constraint": "id IS NOT NULL",
            "severity": "error"
        }
    ],
    metrics=[
        {
            "name": "row_count",
            "value": "COUNT(*)"
        }
    ]
)
def ingest_raw_data(spark, source_path: str) -> DataFrame:
    return spark.read.parquet(source_path)
```

## Core Concepts

### 1. Layer Decorators

The framework provides three main decorators corresponding to the medallion architecture layers:

- `@bronze`: For raw data ingestion with basic validation
- `@silver`: For cleaned and normalized data
- `@gold`: For business-level aggregations

Each decorator can be configured with:
- Data quality expectations
- Metrics to track
- Table properties
- Comments and documentation

Example:
```python
@silver(
    expectations=[
        {
            "name": "no_duplicates",
            "constraint": "COUNT(*) = COUNT(DISTINCT id)",
            "severity": "error"
        }
    ],
    metrics=[
        {
            "name": "duplicate_count",
            "value": "COUNT(*) - COUNT(DISTINCT id)"
        }
    ],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true"
    },
    comment="Cleaned customer data with deduplication"
)
def clean_customer_data(df: DataFrame) -> DataFrame:
    # Transformation logic here
    return cleaned_df
```

### 2. Configuration Templates

The framework includes a template system for reusing common configurations:

```python
from dlt_framework.core.templates import TemplateManager

# Load templates
manager = TemplateManager("path/to/templates")

# Apply template with overrides
@silver(template="base_silver", overrides={
    "table": {
        "name": "customers",
        "expectations": [
            {
                "name": "valid_email",
                "constraint": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
                "severity": "warning"
            }
        ]
    }
})
def transform_customers(df: DataFrame) -> DataFrame:
    return df
```

### 3. Data Quality Expectations

Expectations are SQL-based constraints that validate your data:

```python
expectations = [
    {
        "name": "positive_amount",
        "constraint": "amount > 0",
        "severity": "error",
        "description": "Ensure amount is positive"
    },
    {
        "name": "valid_category",
        "constraint": "category IN ('A', 'B', 'C')",
        "severity": "warning",
        "description": "Check for valid category values"
    }
]
```

### 4. Metrics

Metrics are SQL expressions that measure aspects of your data:

```python
metrics = [
    {
        "name": "total_amount",
        "value": "SUM(amount)",
        "description": "Total transaction amount"
    },
    {
        "name": "category_counts",
        "value": "COUNT(*) GROUP BY category",
        "description": "Count of records per category"
    }
]
```

### 5. Template Examples

The framework includes several built-in templates:

- `base_delta_table`: Basic Delta Lake optimizations
- `base_bronze`: Raw data ingestion patterns
- `base_silver`: Data cleaning patterns
- `base_gold`: Aggregation patterns
- `time_series`: Time series data patterns
- `dimension_table`: Dimension table patterns

Example template usage:
```python
@bronze(template="time_series", overrides={
    "table": {
        "name": "sensor_readings",
        "expectations": [
            {
                "name": "valid_sensor_id",
                "constraint": "sensor_id IS NOT NULL",
                "severity": "error"
            }
        ]
    }
})
def ingest_sensor_data(df: DataFrame) -> DataFrame:
    return df
```

## Best Practices

1. **Layer Organization**:
   - Use `@bronze` for raw data ingestion with minimal transformations
   - Use `@silver` for data cleaning and normalization
   - Use `@gold` for business metrics and aggregations

2. **Data Quality**:
   - Define expectations at each layer
   - Use appropriate severity levels
   - Include descriptive messages in expectations

3. **Metrics**:
   - Track row counts and null counts
   - Monitor business-specific metrics
   - Use statistical metrics where appropriate

4. **Templates**:
   - Create templates for common patterns
   - Use inheritance for shared configurations
   - Override only what's necessary

5. **Delta Lake Integration**:
   - Enable auto-optimize features
   - Configure appropriate retention periods
   - Use table properties for optimization

## Advanced Usage

### 1. Decorator Stacking

You can stack multiple decorators for complex scenarios:

```python
@gold(template="time_series")
@silver(expectations=[...])
def process_data(df: DataFrame) -> DataFrame:
    return df
```

### 2. Dynamic Configuration

Load configurations from YAML/JSON files:

```python
from dlt_framework.core.config import Config

config = Config.from_yaml("config.yaml")
@silver(**config.dict())
def transform_data(df: DataFrame) -> DataFrame:
    return df
```

### 3. Custom Templates

Create your own templates for specific use cases:

```yaml
# custom_templates.yaml
- name: my_template
  description: Custom template for specific use case
  extends: base_silver
  table:
    expectations:
      - name: custom_check
        constraint: "custom_column IS NOT NULL"
        severity: "error"
    metrics:
      - name: custom_metric
        value: "COUNT(DISTINCT custom_column)"
```

## Troubleshooting

Common issues and solutions:

1. **Expectation Failures**:
   - Check the constraint SQL syntax
   - Verify column names and types
   - Review data quality issues

2. **Template Issues**:
   - Ensure template files are in correct format
   - Check template inheritance chain
   - Verify override syntax

3. **Performance Issues**:
   - Review metric complexity
   - Check Delta Lake optimizations
   - Monitor execution plans

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on:
- Submitting issues
- Creating pull requests
- Development setup
- Running tests

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 