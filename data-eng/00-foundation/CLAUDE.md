# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks data engineering project template implementing a medallion architecture (Bronze → Silver → Gold) using 
Decalarative Pipelines (Delta Live Tables (DLT)) and Databricks Asset Bundles for deployment management.

## Key Commands

### Databricks Asset Bundle Operations
```bash
# Initialize Asset Bundle structure (if starting fresh)
databricks bundle init

# Validate bundle configuration
databricks bundle validate

# Deploy to development environment
databricks bundle deploy --target dev

# Deploy to production environment
databricks bundle deploy --target prod
```

### Pipeline Development Workflow
```bash
# Validate Python syntax
python -m py_compile src/pipelines/**/*.py

# Run unit tests (when test files exist)
python -m pytest tests/ -v

# Deploy and test pipeline
databricks bundle deploy --target dev
databricks jobs run-now --job-id <pipeline_name>
```

## Architecture Overview

### Project Structure
- `PRPs/` - Problem Requirements & Proposals for planning features
- `PRPs/templates/` - PRP templates for systematic feature development
- `src/` - Source code (to be created during development)
  - `src/data_generation/` - Synthetic data generation jobs
  - `src/pipelines/` - Delta Live Tables pipeline definitions
    - `src/pipelines/bronze/` - Raw data ingestion
    - `src/pipelines/silver/` - Data cleaning and transformation  
    - `src/pipelines/gold/` - Business-ready dimensional models
  - `src/tests/` - Unit and integration tests

### Data Engineering Patterns
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Delta Live Tables**: Declarative pipeline definitions with data quality expectations
- **Asset Bundle Management**: Infrastructure-as-code for all deployments
- **Environment Isolation**: Separate dev/staging/prod configurations

## Critical Development Patterns

### Asset Bundle Configuration
All infrastructure must be managed through Asset Bundles using `databricks.yml`:
```yaml
variables:
  catalog:
    description: "Unity Catalog name"
  schema:
    description: "Schema name"
  max_files_per_trigger:
    description: "Performance tuning parameter"

targets:
  dev:
    variables:
      catalog: "dev_catalog"
      schema: "dev_schema"
  prod:
    variables:
      catalog: "prod_catalog"
      schema: "prod_schema"
```

### DLT Pipeline Patterns
```python
import dlt
import sys
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Environment-aware configuration with Unity Catalog
CATALOG = spark.conf.get("bundle.target.catalog", "default_catalog")
SCHEMA = spark.conf.get("bundle.target.schema", "default_schema")
PIPELINE_ENV = spark.conf.get("bundle.target.name", "dev")

# Critical path handling pattern (include in all pipeline files)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = "/".join(notebook_path.split("/")[:-2])
    sys.path.insert(0, f"{base_path}/src")
except:
    # Fallback to multiple common paths
    possible_paths = ["/Workspace/src", "/databricks/driver/src", "/repos/src"]
    for path in possible_paths:
        if os.path.exists(path):
            sys.path.insert(0, path)
            break

# Bronze layer with schema enforcement and cloud files
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_events",
    comment="Raw event data ingestion with schema enforcement",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported"
    }
)
@dlt.expect_all_or_drop({
    "valid_event_id": "event_id IS NOT NULL AND LENGTH(event_id) > 0",
    "valid_timestamp": "event_timestamp IS NOT NULL"
})
def bronze_events():
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), False),
        StructField("properties", StringType(), True)
    ])
    
    return (
        spark.readStream
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/tmp/schemas/{PIPELINE_ENV}/events")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(spark.conf.get("source.events.path", "/mnt/data/events/"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )

# Lakeflow Connect pattern for external data sources
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_external_customers",
    comment="External customer data via Lakeflow Connect"
)
def bronze_external_customers():
    return dlt.read_stream("lakeflow.salesforce.customers")

# Silver layer with business logic and enhanced data quality
@dlt.view(name="silver_events_staging")
def silver_events_staging():
    """Staging view for silver transformation"""
    return (
        dlt.read(f"{CATALOG}.{SCHEMA}.bronze_events")
        .filter("_rescued_data IS NULL")  # Filter out malformed records
        .filter("event_timestamp >= current_date() - interval 7 days")  # Recent data only
    )

@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.silver_events",
    comment="Cleaned and enriched event data",
    table_properties={
        "quality": "silver",
        "pipelines.pii.fields": "user_id"
    }
)
@dlt.expect_all_or_drop({
    "valid_event_id": "event_id IS NOT NULL",
    "valid_user_id": "user_id IS NOT NULL OR event_type = 'anonymous'",
    "future_timestamp": "event_timestamp <= current_timestamp()"
})
@dlt.expect_or_fail({
    "no_duplicates": "COUNT(*) = COUNT(DISTINCT event_id)"
})
def silver_events():
    return (
        dlt.read("silver_events_staging")
        .withColumn("event_date", col("event_timestamp").cast("date"))
        .withColumn("hour_bucket", hour(col("event_timestamp")))
        .withColumn("processed_at", current_timestamp())
    )

# Gold layer aggregations with SCD Type 2 support
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.gold_daily_user_metrics",
    comment="Daily user engagement metrics for analytics",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_daily_user_metrics():
    return (
        dlt.read(f"{CATALOG}.{SCHEMA}.silver_events")
        .groupBy("user_id", "event_date")
        .agg(
            count("*").alias("total_events"),
            countDistinct("event_type").alias("unique_event_types"),
            min("event_timestamp").alias("first_event"),
            max("event_timestamp").alias("last_event")
        )
        .withColumn("engagement_score", 
                   when(col("total_events") > 10, "high")
                   .when(col("total_events") > 3, "medium")
                   .otherwise("low"))
    )
```

### Data Quality Expectations & Monitoring
Always implement comprehensive data quality using DLT decorators:

#### Core Expectation Types
- `@dlt.expect_all_or_drop()` - Drop rows that fail expectations (use for critical data quality)
- `@dlt.expect_all()` - Log violations but keep processing (use for monitoring)
- `@dlt.expect_or_drop()` - Drop individual failing rows (use for outlier removal)
- `@dlt.expect_or_fail()` - Fail entire pipeline on violations (use for schema validation)

#### Advanced Quality Patterns
```python
# Multi-level data quality with custom metrics
@dlt.table(name="silver_customers")
@dlt.expect_all_or_drop({
    "valid_customer_id": "customer_id IS NOT NULL AND LENGTH(customer_id) >= 5",
    "valid_email": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
    "valid_created_date": "created_date <= current_date()"
})
@dlt.expect_all({
    "reasonable_age": "age BETWEEN 13 AND 120",
    "complete_profile": "first_name IS NOT NULL AND last_name IS NOT NULL"
})
@dlt.expect_or_fail({
    "no_duplicate_emails": "COUNT(*) = COUNT(DISTINCT email)",
    "schema_compliance": "_rescued_data IS NULL"
})
def silver_customers():
    return dlt.read("bronze_customers")

# Data freshness monitoring
@dlt.expect_all({
    "data_freshness": "max(created_date) >= current_date() - interval 1 day"
})
def gold_daily_summary():
    return dlt.read("silver_events").groupBy("event_date").count()
```

## Development Guidelines

### Mandatory Practices
1. **Asset Bundle First**: Never deploy manually - always use `databricks bundle deploy`
2. **Environment Isolation**: Use Asset Bundle targets for dev/staging/prod separation
3. **Unity Catalog Governance**: Use three-part naming `{catalog}.{schema}.{table}` everywhere
4. **Configuration Variables**: Use `spark.conf.get("bundle.target.*")` with Asset Bundle variables
5. **DLT Dependencies**: Use `dlt.read()` and `dlt.read_stream()` for table dependencies, never `spark.read()`
6. **Data Quality**: Include multi-level `@dlt.expect_*` decorators on all tables
7. **Schema Enforcement**: Define explicit schemas for bronze layer ingestion
8. **Path Handling**: Include comprehensive path resolution in pipeline files
9. **PII Handling**: Mark PII fields in table properties for governance compliance
10. **Change Data Capture**: Enable CDC on gold tables with `delta.enableChangeDataFeed`

### Planning Process
Before implementing features:
1. Create a PRP using templates in `PRPs/templates/prp_base.md`
2. Review `INITIAL.md` for project-specific requirements and patterns
3. Use the comprehensive PRP template which includes validation loops and context requirements
4. Follow the Asset Bundle deployment workflow

### Common Pitfalls to Avoid
- **Using `spark.read()` instead of `dlt.read()`** for dependencies (breaks DLT lineage)
- **Missing Unity Catalog three-part naming** `{catalog}.{schema}.{table}` format
- **Hardcoding workspace URLs or paths** in configurations (use Asset Bundle variables)
- **Missing data quality expectations** on tables (mandatory for production pipelines)
- **Using `display()` in DLT functions** (return DataFrames instead)
- **Deploying directly to production** without dev environment testing
- **Creating manual utility modules** instead of using Asset Bundle variables
- **Skipping schema enforcement** in bronze layer (causes downstream issues)
- **Missing PII governance markers** in table properties
- **Not handling `_rescued_data`** in silver layer transformations
- **Using streaming without appropriate triggers** (can cause performance issues)
- **Missing resource dependencies** in Asset Bundle configuration

### Testing Strategy
1. **Syntax Validation**: `databricks bundle validate --target dev`
2. **Unit Tests**: Test pipeline logic with mocked DLT dependencies using pytest
3. **Schema Validation**: `python -m py_compile src/pipelines/**/*.py`
4. **Integration Tests**: Deploy to dev environment and validate end-to-end pipeline execution
5. **Data Quality Validation**: Verify DLT expectations work correctly with test data
6. **Performance Testing**: Monitor pipeline execution metrics and optimize triggers
7. **Lineage Validation**: Verify table dependencies and Unity Catalog governance

#### Essential Test Commands
```bash
# Complete validation workflow
databricks bundle validate --target dev
python -m py_compile src/pipelines/**/*.py
databricks bundle deploy --target dev
databricks jobs run-now --job-id <pipeline_job_id>

# Data quality monitoring
databricks sql query "SELECT * FROM system.event_log.dlt_pipeline_events WHERE pipeline_id = '<pipeline_id>'"

# Unity Catalog validation
databricks catalogs get <catalog_name>
databricks schemas list --catalog-name <catalog>
databricks tables list --catalog-name <catalog> --schema-name <schema>
```

## Key Files and Their Purpose

- `INITIAL.md` - Comprehensive project documentation and setup guide
- `PRPs/prp_base.md` - Template for systematic feature planning with AI agents
- `databricks.yml` - Asset Bundle configuration (to be created)
- `resources/pipelines.yml` - Pipeline resource definitions (to be created)
- Pipeline files should return DataFrames and use DLT decorators properly

This project emphasizes modern data engineering practices using Databricks platform capabilities with enterprise-grade deployment management through Asset Bundles.