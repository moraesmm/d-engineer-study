# Data Engineering Study

A hands-on repository for learning Data Engineering with PySpark, Apache Iceberg, and AWS.

## Modules

### 01 - PySpark Basics
PySpark fundamentals: SparkSession, DataFrames, transformations, aggregations, window functions, and joins.

### 02 - Spark SQL
SQL queries in Spark: temporary views, CTEs, subqueries, date/string functions, and set operations.

### 03 - Partitioning & Compression
Partitioning strategies (repartition, coalesce, partitionBy, bucketing) and file formats (Parquet, ORC, JSON, CSV).

### 04 - PyIceberg
Apache Iceberg: catalogs, time travel, schema evolution, partition evolution, and table maintenance.

### 05 - AWS Integration
AWS integration: S3 read/write operations, Glue Catalog, Crawlers, and Athena queries.

## Project Structure

```
├── 01_pyspark_basics/
│   ├── 01_spark_session.py
│   ├── 02_dataframes_transformations.py
│   └── pyspark_basics.ipynb
├── 02_spark_sql/
│   ├── 01_spark_sql_basics.py
│   └── spark_sql.ipynb
├── 03_partitioning_compression/
│   ├── 01_partitioning.py
│   ├── 02_compression_formats.py
│   └── partitioning_compression.ipynb
├── 04_pyiceberg/
│   └── pyiceberg.ipynb
└── 05_aws_integration/
    ├── aws_s3_spark.ipynb
    └── aws_glue_athena.ipynb
```

## Requirements

- Python 3.9+
- PySpark 3.5+
- PyIceberg
- boto3 (for AWS)

## Usage

Each module can be studied independently. The `.py` files contain commented examples and the `.ipynb` notebooks provide an interactive experience.
