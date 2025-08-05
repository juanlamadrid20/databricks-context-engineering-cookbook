# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks data engineering project template implementing a medallion architecture (Bronze → Silver → Gold) using 
Declarative Pipelines (Delta Live Tables (DLT)) and Databricks Asset Bundles for deployment management.

## Consolidated Documentation References

### External Documentation Hub
Use these documentation sources as the single source of truth for all development activities:

```yaml
# Primary Databricks Documentation
databricks_core:
  asset_bundles:
    - url: https://docs.databricks.com/dev-tools/bundles/index.html
      usage: "Asset Bundle configuration, deployment patterns, and environment management"
    - url: https://docs.databricks.com/dev-tools/cli/bundle-cli.html
      usage: "Asset Bundle CLI commands and workflow automation"
      
  delta_live_tables:
    - url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
      usage: "DLT decorators (@dlt.table, @dlt.expect_*), streaming tables, and pipeline patterns"
    - url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html
      usage: "Data quality expectations, quarantine patterns, and validation strategies"
    - url: https://docs.databricks.com/workflows/delta-live-tables/index.html
      usage: "General DLT concepts, medallion architecture, and best practices"
      
  unity_catalog:
    - url: https://docs.databricks.com/data-governance/unity-catalog/best-practices.html
      usage: "Three-part naming conventions, governance patterns, and PII handling"
    - url: https://docs.databricks.com/data-governance/unity-catalog/index.html
      usage: "Unity Catalog setup, permissions, and data governance framework"
      
  performance_optimization:
    - url: https://docs.databricks.com/delta/optimize.html
      usage: "Delta Lake optimization, Z-ordering, and liquid clustering strategies"
    - url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-performance.html
      usage: "DLT pipeline performance tuning and scaling best practices"

# Platform Documentation  
platform_docs:
  delta_lake:
    - url: https://docs.delta.io/
      usage: "Delta Lake core concepts, ACID transactions, and time travel"
  medallion_architecture:
    - url: https://www.databricks.com/glossary/medallion-architecture
      usage: "Bronze-Silver-Gold architecture patterns and implementation strategies"
```

### Reference Usage Guidelines
- **For PRPs**: Reference this section instead of duplicating URLs in individual PRP files
- **For Implementation**: Use as single source for all Databricks documentation links
- **For Context Engineering**: Combine with domain-specific documentation in PRPs as needed

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

### Asset Bundle Management - Single Source of Truth

#### Asset Bundle Workflow Commands
```bash
# Initialize bundle (if starting fresh)
databricks bundle init

# Validate bundle configuration
databricks bundle validate

# Deploy to development environment
databricks bundle deploy --target dev

# Deploy to production environment
databricks bundle deploy --target prod
```

#### Comprehensive Root `databricks.yml` Template
```yaml
bundle:
  name: <TODO: project_name>-pipeline
  
variables:
  catalog:
    description: "Unity Catalog name"
    
  schema:
    description: "Schema name within catalog"
    default: "data_eng"
    
  environment:
    description: "Environment identifier (dev/prod)"
    
  volumes_path:
    description: "Path to Databricks Volumes for data ingestion"
    
  max_files_per_trigger:
    description: "Auto Loader performance tuning"
    default: 100

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    variables:
      catalog: "<TODO: dev_catalog>"
      schema: "<TODO: schema_name>"
      environment: "dev"
      volumes_path: "/Volumes/<TODO: dev_catalog>/<TODO: schema_name>/raw_data"
      max_files_per_trigger: 50
      
  prod:
    mode: production
    variables:
      catalog: "<TODO: prod_catalog>"
      schema: "<TODO: schema_name>" 
      environment: "prod"
      volumes_path: "/Volumes/<TODO: prod_catalog>/<TODO: schema_name>/raw_data"
      max_files_per_trigger: 200
```

#### DLT Pipeline Resource Configuration (SERVERLESS ONLY)
```yaml
# resources/pipelines.yml
resources:
  pipelines:
    <TODO: pipeline_name>:
      name: "<TODO: project_name>-pipeline-${var.environment}"
      target: "${var.schema}"  # CRITICAL: Only schema when catalog is specified separately
      catalog: "${var.catalog}"  # CRITICAL: Required for serverless compute
      serverless: true  # MANDATORY: Must be true - NO cluster configurations allowed
      
      libraries:
        # CRITICAL: Reference individual files, NOT directories
        # Shared schemas
        - file:
            path: ../<relative_path>/shared/<TODO: usecase_name>_schemas.py
        # Bronze layer ingestion
        - file:
            path: ../<relative_path>/bronze/<TODO: table1>_ingestion.py
        - file:
            path: ../<relative_path>/bronze/<TODO: table2>_ingestion.py
        - file:
            path: ../<relative_path>/bronze/<TODO: table3>_ingestion.py
        # Silver layer transformations
        - file:
            path: ../<relative_path>/silver/<TODO: table1>_transform.py
        - file:
            path: ../<relative_path>/silver/<TODO: table2>_transform.py
        # Gold layer dimensions
        - file:
            path: ../<relative_path>/gold/<TODO: dimension1>.py
            
      configuration:
        "bundle.sourcePath": "/Workspace${workspace.file_path}/src"
        "CATALOG": "${var.catalog}"
        "SCHEMA": "${var.schema}"
        "PIPELINE_ENV": "${var.environment}"
        "VOLUMES_PATH": "${var.volumes_path}"
        "MAX_FILES_PER_TRIGGER": "${var.max_files_per_trigger}"
        # compliance configurations (serverless handles compute automatically)
        # "spark.databricks.delta.properties.defaults.encryption.enabled": "true" # TODO: Claud.md review
        "spark.databricks.delta.properties.defaults.changeDataFeed.enabled": "true"
        # Compliance and governance metadata (stored as configuration parameters)
        "compliance": "<TODO: compliance_framework>"
        "data_classification": "<TODO: data_classification>"
        "environment": "${var.environment}"
        
      # Continuous processing for workloads  
      continuous: false
```

#### Serverless Job Configuration Pattern (CORRECT - NO CLUSTER CONFIGS)
```yaml
# resources/jobs.yml - CORRECT SERVERLESS PATTERN (default when no cluster specified)
resources:
  jobs:
    example_job:
      name: "job-name-${var.environment}"
      # ✅ CORRECT: NO cluster configuration = serverless by default
      
      tasks:
        - task_key: "task_name"
          # ✅ CORRECT: No compute_key or cluster references
          notebook_task:
            notebook_path: "../src/path/to/notebook"
            base_parameters:
              catalog: "${var.catalog}"
              schema: "${var.schema}"
          
        - task_key: "another_task"
          python_wheel_task:
            package_name: "my_package"
            entry_point: "main_function"
            parameters:
              - "--param=value"
```

#### ❌ FORBIDDEN JOB ANTI-PATTERNS (NEVER CREATE THESE)
```yaml
# ❌ ALL OF THESE PATTERNS ARE FORBIDDEN - OMIT CLUSTER CONFIGS FOR SERVERLESS
resources:
  jobs:
    forbidden_job_example:
      name: "bad-example"
      
      # ❌ NEVER: job_clusters section
      job_clusters:
        - job_cluster_key: "any_key"
          new_cluster:
            spark_version: "any_version"
            node_type_id: "any_type"
            
      # ❌ NEVER: new_cluster section at job level
      new_cluster:
        spark_version: "any_version"
        node_type_id: "any_type"
        num_workers: 2
        
      # ❌ NEVER: existing_cluster_id reference
      existing_cluster_id: "cluster-id"
      
      # ❌ NEVER: compute specifications in tasks
      tasks:
        - task_key: "bad_task"
          existing_cluster_id: "cluster-id"  # FORBIDDEN
          job_cluster_key: "cluster_key"      # FORBIDDEN
```

#### Asset Bundle Variable Management Pattern
```yaml
variables:
  catalog:
    description: "Unity Catalog name"
    default: "<TODO: default_catalog>"
  schema:
    description: "Schema name for data"
    default: "<TODO: schema_name>"
  volumes_path:
    description: "Path to Databricks Volumes for data files"
    default: "/Volumes/<TODO: default_catalog>/<TODO: schema_name>/raw_data"
  max_files_per_trigger:
    description: "Auto Loader performance tuning parameter"
    default: 100

targets:
  dev:
    variables:
      catalog: "<TODO: dev_catalog>"
      schema: "<TODO: schema_name>"
      volumes_path: "/Volumes/<TODO: dev_catalog>/<TODO: schema_name>/raw_data"
      max_files_per_trigger: 50
  prod:
    variables:
      catalog: "<TODO: prod_catalog>"
      schema: "<TODO: schema_name>"
      volumes_path: "/Volumes/<TODO: prod_catalog>/<TODO: schema_name>/raw_data"
      max_files_per_trigger: 200
```

#### Pipeline Configuration Pattern
```python
# Environment-aware configuration loading - CRITICAL pattern for all pipeline files
CATALOG = spark.conf.get("CATALOG", "<TODO: default_catalog>")
SCHEMA = spark.conf.get("SCHEMA", "<TODO: schema_name>")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/<TODO: default_catalog>/<TODO: schema_name>/raw_data")
MAX_FILES_PER_TRIGGER = spark.conf.get("MAX_FILES_PER_TRIGGER", "100")
```

#### Critical Path Handling Pattern
```python
# CRITICAL: Include this pattern in all pipeline files
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
```

### DLT Pipeline Patterns

#### Critical Autoloader Pattern for DLT Streaming
**🚨 MANDATORY**: When using Autoloader (`cloudFiles`) with DLT streaming tables, you **MUST** include `.format("cloudFiles")`:

```python
# ✅ CORRECT - Required format for Autoloader with DLT
@dlt.table(name="bronze_table_raw")
def bronze_table_raw():
    return (
        spark.readStream.format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "<TODO: file_format>")
        .option("header", "true")  # Only for CSV format
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/<TODO: table_name>")
        .schema(TABLE_SCHEMA)
        .load(f"{VOLUMES_PATH}")
    )

# ❌ INCORRECT - Missing .format("cloudFiles") 
@dlt.table(name="bronze_table_raw")
def bronze_table_raw():
    return (
        spark.readStream  # ← MISSING: .format("cloudFiles")
        .option("cloudFiles.format", "<TODO: file_format>")  # This alone is not sufficient
        .option("header", "true")  # Only for CSV format
        .load(f"{VOLUMES_PATH}")
    )
```

**Why this matters:**
- DLT requires explicit `.format("cloudFiles")` to recognize and optimize Autoloader functionality
- Missing this pattern causes pipeline failures and performance issues
- Simply having `"cloudFiles.format"` option is **not sufficient** - you need both

#### Complete DLT Pipeline Template
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
    name="bronze_<TODO: table_name>",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Raw <TODO: data_type> data ingestion with schema enforcement",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported"
    }
)
@dlt.expect_all_or_drop({
    "valid_<TODO: primary_key>": "<TODO: primary_key> IS NOT NULL AND LENGTH(<TODO: primary_key>) > 0",
    "valid_timestamp": "<TODO: timestamp_field> IS NOT NULL"
})
def bronze_<TODO: table_name>():
    schema = StructType([
        StructField("<TODO: primary_key>", StringType(), False),
        StructField("<TODO: field1>", StringType(), True),
        StructField("<TODO: field2>", StringType(), True),
        StructField("<TODO: timestamp_field>", TimestampType(), False),
        StructField("<TODO: field3>", StringType(), True)
    ])
    
    return (
        spark.readStream
        .option("cloudFiles.format", "<TODO: file_format>")
        .option("cloudFiles.schemaLocation", f"/tmp/schemas/{PIPELINE_ENV}/<TODO: table_name>")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(spark.conf.get("source.<TODO: table_name>.path", "/mnt/data/<TODO: table_name>/"))
        .select("*", "_metadata")  # Include file metadata
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
        .drop("_metadata")  # Clean up after extracting needed fields
    )

# Lakeflow Connect pattern for external data sources
@dlt.table(
    name="bronze_external_<TODO: source_name>",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="External <TODO: data_type> data via Lakeflow Connect"
)
def bronze_external_<TODO: source_name>():
    return dlt.read_stream("lakeflow.<TODO: source_system>.<TODO: source_table>")

# Silver layer with business logic and enhanced data quality
@dlt.view(name="silver_<TODO: table_name>_staging")
def silver_<TODO: table_name>_staging():
    """Staging view for silver transformation"""
    return (
        dlt.read("bronze_<TODO: table_name>")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("_rescued_data IS NULL")  # Filter out malformed records
        .filter("<TODO: timestamp_field> >= current_date() - interval 7 days")  # Recent data only
    )

@dlt.table(
    name="silver_<TODO: table_name>",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Cleaned and enriched <TODO: data_type> data",
    table_properties={
        "quality": "silver",
        "pipelines.pii.fields": "<TODO: pii_field_list>"
    }
)
@dlt.expect_all_or_drop({
    "valid_<TODO: primary_key>": "<TODO: primary_key> IS NOT NULL",
    "valid_<TODO: field>": "<TODO: field> IS NOT NULL OR <TODO: condition_field> = '<TODO: condition_value>'",
    "future_timestamp": "<TODO: timestamp_field> <= current_timestamp()"
})
@dlt.expect_or_fail({
    "no_duplicates": "COUNT(*) = COUNT(DISTINCT <TODO: primary_key>)"
})
def silver_<TODO: table_name>():
    return (
        dlt.read("silver_<TODO: table_name>_staging")
        .withColumn("<TODO: date_field>", col("<TODO: timestamp_field>").cast("date"))
        .withColumn("hour_bucket", hour(col("<TODO: timestamp_field>")))
        .withColumn("processed_at", current_timestamp())
    )

# Gold layer aggregations with SCD Type 2 support
@dlt.table(
    name="gold_daily_<TODO: metric_name>",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Daily <TODO: metric_description> metrics for analytics",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_daily_<TODO: metric_name>():
    return (
        dlt.read("silver_<TODO: table_name>")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy("<TODO: groupby_field1>", "<TODO: date_field>")
        .agg(
            count("*").alias("total_<TODO: count_metric>"),
            countDistinct("<TODO: distinct_field>").alias("unique_<TODO: distinct_metric>"),
            min("<TODO: timestamp_field>").alias("first_<TODO: timestamp_alias>"),
            max("<TODO: timestamp_field>").alias("last_<TODO: timestamp_alias>")
        )
        .withColumn("<TODO: score_column>", 
                   when(col("total_<TODO: count_metric>") > 10, "high")
                   .when(col("total_<TODO: count_metric>") > 3, "medium")
                   .otherwise("low"))
    )
```

### File Metadata Patterns

**CRITICAL**: Always use the `_metadata` column for file information instead of deprecated functions like `input_file_name()`.

#### Correct File Metadata Pattern
```python
# Correct approach using _metadata column
return (
    spark.readStream
    .option("cloudFiles.format", "<TODO: file_format>")
    .option("header", "true")  # Only for CSV format
    .schema(schema)
    .load(f"{VOLUMES_PATH}/<TODO: file_pattern>")
    .select("*", "_metadata")  # REQUIRED: Explicitly select metadata
    .withColumn("_file_name", col("_metadata.file_name"))
    .withColumn("_file_path", col("_metadata.file_path"))
    .withColumn("_file_size", col("_metadata.file_size"))
    .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
    .drop("_metadata")  # Clean up temporary column
)
```

#### Deprecated Pattern (DO NOT USE)
```python
# ❌ DEPRECATED - Do not use input_file_name()
from pyspark.sql.functions import input_file_name
.withColumn("_file_name", input_file_name())  # This is deprecated
```

#### Available Metadata Fields
- `_metadata.file_name`: File name with extension
- `_metadata.file_path`: Full file path
- `_metadata.file_size`: File size in bytes
- `_metadata.file_modification_time`: Last modification timestamp
- `_metadata.file_block_start`: Start offset of block being read
- `_metadata.file_block_length`: Length of block being read

Reference: [Databricks File Metadata Column Documentation](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)

### Data Quality Expectations & Monitoring
Always implement comprehensive data quality using DLT decorators:

- **IMPORTANT**: Aggregate checks must **NOT** be used inside expectations. Expectations must be row-level Boolean expressions only.

#### Core Expectation Types
- `@dlt.expect_all_or_drop()` - Drop rows that fail expectations (use for critical data quality)
- `@dlt.expect_all()` - Log violations but keep processing (use for monitoring)
- `@dlt.expect_or_drop()` - Drop individual failing rows (use for outlier removal)
- `@dlt.expect_or_fail()` - Fail entire pipeline on violations (use for schema validation)

#### Advanced Quality Patterns
Note: Do not use aggregate expressions like COUNT(), SUM(), or AVG() inside @dlt.expect_* decorators. Use a separate metrics table for aggregate validations.
```python
# Multi-level data quality with custom metrics
@dlt.table(name="silver_<TODO: table_name>")
@dlt.expect_all_or_drop({
    "valid_<TODO: primary_key>": "<TODO: primary_key> IS NOT NULL AND LENGTH(<TODO: primary_key>) >= 5",
    "valid_email": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
    "valid_created_date": "created_date <= current_date()"
})
@dlt.expect_all({
    "reasonable_<TODO: field>": "<TODO: field> BETWEEN <TODO: min_value> AND <TODO: max_value>",
    "complete_profile": "<TODO: required_field1> IS NOT NULL AND <TODO: required_field2> IS NOT NULL"
})
@dlt.expect_or_fail({
    "no_duplicate_emails": "COUNT(*) = COUNT(DISTINCT email)",
    "schema_compliance": "_rescued_data IS NULL"
})
def silver_<TODO: table_name>():
    return dlt.read("bronze_<TODO: table_name>")

# Data freshness monitoring
@dlt.expect_all({
    "data_freshness": "max(created_date) >= current_date() - interval 1 day"
})
def gold_daily_summary():
    return dlt.read("silver_<TODO: table_name>").groupBy("<TODO: date_field>").count()
```

## Development Guidelines

### Mandatory Practices

#### Compute Requirements (🚨 SERVERLESS ONLY - NO EXCEPTIONS)
1. **Serverless by Omission**: ALL jobs use serverless compute by NOT specifying any cluster configurations
2. **Forbidden Cluster Sections**: NEVER create `job_clusters:`, `new_cluster:`, `existing_cluster_id:` in job definitions
3. **DLT Serverless**: Pipelines must specify `serverless: true` with NO `clusters:` section
4. **No Task-Level Clusters**: Tasks must NOT include `job_cluster_key:`, `existing_cluster_id:`, or `new_cluster:` fields
5. **Forbidden Cluster Fields**: NEVER use `node_type_id`, `num_workers`, `spark_version`, `driver_node_type_id` anywhere

#### Asset Bundle Requirements
5. **Asset Bundle First**: Never deploy manually - always use `databricks bundle deploy`
6. **Environment Isolation**: Use Asset Bundle targets for dev/staging/prod separation  
7. **Version Control**: All databricks.yml configurations must be version controlled
8. **Configuration via Variables**: Use Asset Bundle variables and `spark.conf.get()` pattern
9. **Include Pattern**: Use `include: - resources/*.yml` for resource organization
10. **Variable Defaults**: Always provide sensible defaults for Asset Bundle variables

#### Pipeline Development Standards  
11. **DLT Table Names**: Use simple table names in `@dlt.table(name="table_name")` decorators - NO full namespace format
12. **DLT Table References**: Use simple table names in `dlt.read("table_name")` calls - NO full namespace format
13. **Unity Catalog**: Catalog and schema are specified at pipeline level in Asset Bundle configuration
13. **Configuration Variables**: Use `spark.conf.get("CATALOG", "default")` with Asset Bundle variables
14. **DLT Dependencies**: Use `dlt.read()` and `dlt.read_stream()` for table dependencies, never `spark.read()`
15. **Data Quality**: Include multi-level `@dlt.expect_*` decorators on all tables
16. **Schema Enforcement**: Define explicit schemas for bronze layer ingestion
17. **Path Handling**: Include comprehensive path resolution in pipeline files
18. **PII Handling**: Mark PII fields in table properties for governance compliance
19. **Change Data Capture**: Enable CDC on gold tables with `delta.enableChangeDataFeed`

### 🚨 DLT Streaming Ingestion Best Practice

**Always use `@dlt.table` (not `@dlt.view`) for raw ingestion from Autoloader (`spark.readStream` / `cloudFiles`).**

- `@dlt.table` ensures streaming tables are **persisted**, **checkpointed**, and **visible** to downstream DLT pipelines.
- `@dlt.view` creates **non-materialized logic**, which:
  - Is **not checkpointed or persisted**,
  - May **not register** in Unity Catalog,
  - Can lead to downstream **`Dataset not defined`** errors when downstream flows call `dlt.read_stream(...)`.

### Planning Process
Before implementing features:
1. Create a PRP using templates in `PRPs/templates/prp_base.md`
2. Review `INITIAL.md` for project-specific requirements and patterns
3. Use the comprehensive PRP template which includes validation loops and context requirements
4. Follow the Asset Bundle deployment workflow

### Common Pitfalls to Avoid

**🚨 CRITICAL: SERVERLESS COMPUTE ONLY** — Never create cluster configurations. All pipelines and jobs must use serverless compute.

**Avoid placing aggregate checks in expectations** — move them to a metrics table instead.

#### Asset Bundle Configuration Pitfalls
- **Using cluster configurations** - NEVER create job_clusters, new_cluster, or clusters sections (MUST use serverless only)
- **Manual deployment** instead of using Asset Bundles
- **Missing environment separation** in Asset Bundle configuration
- **Hard-coding workspace URLs** or environment-specific values in databricks.yml
- **Creating unused utility modules** - prefer Asset Bundle variables + `spark.conf.get()` pattern
- **Missing path handling** in pipeline files - include comprehensive path resolution
- **Missing resource dependencies** in Asset Bundle configuration
- **Not using variable defaults** in Asset Bundle configuration
- **Hardcoding values** that should be Asset Bundle variables
- **Referencing directories in pipeline libraries** - use individual file paths, not directory paths (causes "unable to determine if path is not a notebook" error)
- **Using custom cluster labels** - only 'default', 'updates', and 'maintenance' labels are allowed in DLT pipelines
- **Mixing serverless and cluster configurations** - cannot use both `serverless: true` and `clusters:` section (SERVERLESS ONLY)
- **Missing catalog for serverless pipelines** - must specify both `target: "${var.schema}"` AND `catalog: "${var.catalog}"` for serverless compute (target should only contain schema when catalog is specified separately)
- **Incorrect target configuration** - when `catalog` is specified separately, `target` should only contain the schema name (`target: "${var.schema}"`), NOT the full catalog.schema path (`target: "${var.catalog}.${var.schema}"`)
- **Using invalid pipeline fields** - `tags:` field is not valid in DLT pipeline configuration (use configuration parameters instead)

#### Pipeline Development Pitfalls
- **Using `spark.read()` instead of `dlt.read()`** for dependencies (breaks DLT lineage)
- **🚨 Using full namespace in dlt.read()** - use `dlt.read("table_name")` NOT `dlt.read(f"{CATALOG}.{SCHEMA}.table_name")`
- **Missing Unity Catalog three-part naming** in `@dlt.table(name=...)` decorators (but NOT in `dlt.read()` calls)
- **Missing data quality expectations** on tables (mandatory for production pipelines)
- **Using `display()` in DLT functions** (return DataFrames instead)
- **Deploying directly to production** without dev environment testing
- **Skipping schema enforcement** in bronze layer (causes downstream issues)
- **Missing PII governance markers** in table properties
- **Not handling `_rescued_data`** in silver layer transformations
- **Using streaming without appropriate triggers** (can cause performance issues)
- **Using deprecated `input_file_name()` function** - use `_metadata` column instead (see: https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
- **🚨 Using invalid `databricks sql` commands** - use `databricks api post /api/2.0/sql/statements` instead

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

# Data quality monitoring using SQL API
databricks api post /api/2.0/sql/statements \
  --json '{
    "statement": "SELECT * FROM system.event_log.dlt_pipeline_events WHERE pipeline_id = '\'<pipeline_id>\'",
    "warehouse_id": "<TODO: warehouse_id>"
  }'

# Table validation using SQL API
databricks api post /api/2.0/sql/statements \
  --json '{
    "statement": "SELECT COUNT(*) as record_count FROM <catalog>.<schema>.<table_name>",
    "warehouse_id": "<TODO: warehouse_id>"
  }'

# Unity Catalog validation using CLI
databricks catalogs get <catalog_name>
databricks schemas list <catalog>
databricks tables list <catalog> <schema>

# Pipeline status monitoring
databricks pipelines list-pipelines --filter "name LIKE '%<pipeline_name>%'"
databricks pipelines get <pipeline_id>
```

## Key Files and Their Purpose

- `INITIAL.md` - Comprehensive project documentation and setup guide
- `PRPs/prp_base.md` - Template for systematic feature planning with AI agents
- `databricks.yml` - Asset Bundle configuration (to be created)
- `resources/pipelines.yml` - Pipeline resource definitions (to be created)
- Pipeline files should return DataFrames and use DLT decorators properly

This project emphasizes modern data engineering practices using Databricks platform capabilities with enterprise-grade deployment management through Asset Bundles. 