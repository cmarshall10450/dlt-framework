# DLT Medallion Framework

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/pyspark-3.3+-orange.svg)](https://spark.apache.org/docs/latest/api/python/index.html)

A Python framework for building robust data pipelines using the medallion architecture pattern with Delta Lake tables. This framework provides a simple, decorator-based API for implementing data quality checks, metrics tracking, and metadata management.

## Features

- 🏅 **Layer-specific Decorators**: Easy-to-use `@bronze`, `@silver`, and `@gold` decorators
- 🎯 **Data Quality**: Built-in support for data quality expectations and metrics
- 📝 **Templates**: Reusable configuration templates for common patterns
- 🔄 **Delta Lake Integration**: Seamless integration with Delta Lake features
- 📊 **Metadata Tracking**: Automatic tracking of data lineage and quality metrics

## Quick Start

1. Install the package:
```bash
pip install dlt-framework
```

2. Use the decorators in your code:
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

## Documentation

- [User Guide](docs/user_guide.md): Comprehensive guide to using the framework
- [API Reference](docs/api_reference.md): Detailed API documentation
- [Examples](examples/): Example pipelines and use cases
- [Contributing](CONTRIBUTING.md): Guidelines for contributing to the project

## Requirements

- Python 3.8+
- Apache Spark 3.3+
- Delta Lake 2.0+

## Installation

From PyPI:
```bash
pip install dlt-framework
```

From source:
```bash
git clone https://github.com/yourusername/dlt-framework.git
cd dlt-framework
pip install -e .
```

## Example

Here's a simple example of a data pipeline using the framework:

```python
from dlt_framework.decorators import bronze, silver, gold
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Bronze layer: Raw data ingestion
@bronze(template="base_bronze")
def ingest_orders(spark, source_path: str) -> DataFrame:
    return spark.read.parquet(source_path)

# Silver layer: Data cleaning
@silver(
    expectations=[
        {
            "name": "valid_order",
            "constraint": "amount > 0 AND customer_id IS NOT NULL",
            "severity": "error"
        }
    ]
)
def clean_orders(df: DataFrame) -> DataFrame:
    return df.filter(col("amount") > 0)

# Gold layer: Business metrics
@gold(
    metrics=[
        {
            "name": "total_amount",
            "value": "SUM(amount)"
        },
        {
            "name": "customer_count",
            "value": "COUNT(DISTINCT customer_id)"
        }
    ]
)
def calculate_metrics(df: DataFrame) -> DataFrame:
    return df.groupBy("date").agg(...)
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on:
- Submitting issues
- Creating pull requests
- Development setup
- Running tests

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- The Apache Spark and Delta Lake communities
- Contributors to the project
- Users who provide valuable feedback 