# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks data engineering project template implementing a medallion architecture (Bronze → Silver → Gold) using Declarative Pipelines (Delta Live Tables/DLT) and Databricks Asset Bundles for deployment management. This project follows This is a **PRP (Product Requirement Prompt) Framework**, not a traditional software project. The core concept: **"PRP = PRD + curated codebase intelligence + agent/runbook"** - designed to enable AI agents to ship production-ready code on the first pass.

### AI Documentation Curation

- `PRPs/ai_docs/` contains curated Claude Code documentation for context injection
- `claude_md_files/` provides framework-specific CLAUDE.md examples

## Critical Success Patterns

### PRP Methodology and Core Principles
1. **Context is King**: Include ALL necessary documentation, examples, and caveats
2. **Validation Loops**: Provide executable tests/lints the AI can run and fix
3. **Information Dense**: Use keywords and patterns from the Databricks ecosystem
4. **Progressive Success**: Start simple, validate, then enhance
5. **Global rules**: Be sure to follow all rules in CLAUDE.md
6. **Asset Bundle First**: All infrastructure should be managed through Databricks Asset Bundles

### PRP Structure Requirements

- **Goal**: Specific end state and desires
- **Why**: Business value and user impact
- **What**: User-visible behavior and technical requirements
- **All Needed Context**: Documentation URLs, code examples, gotchas, patterns
- **Implementation Blueprint**: Pseudocode with critical details and task lists
- **Validation Loop**: Executable commands for syntax, tests, integration

### Key Claude Commands

- `/prp-base-create` - Generate comprehensive PRPs with research
- `/prp-base-execute` - Execute PRPs against codebase

### When Executing PRPs

1. **Load PRP**: Read and understand all context and requirements
2. **ULTRATHINK**: Create comprehensive plan, break down into todos, use subagents, batch tool etc check prps/ai_docs/
3. **Execute**: Implement following the blueprint
4. **Validate**: Run each validation command, fix failures
5. **Complete**: Ensure all checklist items done

## Documentation & References

```yaml
# MUST READ - Include these in your context window
- url: https://docs.databricks.com
  why: "Databricks Specific Features and Documentation"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
  why: "Lakeflow Declarative Pipline Syntax, decorators (@dlt.table, @dlt.expect_*), streaming tables, and pipeline patterns"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
  why: "Lakeflow Declarative Pipline Syntax, decorators (@dlt.table, @dlt.expect_*), streaming tables, and pipeline patterns"

- url: https://databrickslabs.github.io/dbldatagen/public_docs/index.html
  why: "For synthetic data generation, examples, and pattern to use for the Medallion Framework"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html
  why: "Data quality expectations, quarantine patterns, and validation strategies for Lakeflow Declarative Pipelines"

- url: https://docs.databricks.com/workflows/delta-live-tables/index.html
  why: "General LDP concepts, medallion architecture, and best practices"

- url: https://docs.databricks.com/data-governance/unity-catalog/best-practices.html
  why: "Three-part naming conventions, governance patterns, and PII handling"

- url: https://docs.databricks.com/data-governance/unity-catalog/index.html
  why: "Unity Catalog setup, permissions, and data governance framework"

- url: https://docs.databricks.com/delta/optimize.html
  why: "Delta Lake optimization, Z-ordering, and liquid clustering strategies"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-performance.html
  why: "LDP pipeline performance tuning and scaling best practices"

- url: https://docs.delta.io/
  why: "Delta Lake core concepts, ACID transactions, and time travel"

- url: https://www.databricks.com/glossary/medallion-architecture
  why: "Bronze-Silver-Gold architecture patterns and implementation strategies"
```

## Key Commands

```bash
# Asset Bundle Operations
databricks bundle init                    # Initialize bundle structure
databricks bundle validate               # Validate configuration
databricks bundle deploy --target dev    # Deploy to development
databricks bundle deploy --target prod   # Deploy to production

# Pipeline Development Workflow
python -m py_compile src/pipelines/**/*.py    # Validate Python syntax
python -m pytest tests/ -v                   # Run unit tests
databricks bundle deploy --target dev         # Deploy and test
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

#### Comprehensive Root `databricks.yml` Template

```yaml
bundle:
  name: health-insurance-patient-pipeline
variables:
  catalog:
    description: "Unity Catalog name"
  schema:
    description: "Schema name within catalog"
    default: "data_eng"
  environment:
    description: "Environment identifier (dev/prod)"
  volumes_path:
    description: "Path to Databricks Volumes for CSV ingestion"
  max_files_per_trigger:
    description: "Auto Loader performance tuning"
    default: 100
include:
  - resources/*.yml

targets:
  dev:
    mode: development
    variables:
      catalog: "{catalog_name}"
      schema: "{schema_name}"
      environment: "dev"
      volumes_path: "/Volumes/{catalog_name}/{schema_name}/raw_data"
      max_files_per_trigger: 50
      
  prod:
    mode: production
    variables:
      catalog: "{catalog_name}"
      schema: "{schema_name}"
      environment: "prod"
      volumes_path: "/Volumes/{catalog_name}/{schema_name}/raw_data"
      max_files_per_trigger: 200
```

#### DLT Pipeline Resource Configuration (SERVERLESS ONLY)
```yaml
# resources/pipelines.yml
resources:
  pipelines:
    patient_data_pipeline:
      name: "health-insurance-patient-pipeline-${var.environment}"
      target: "${var.schema}"  # CRITICAL: Only schema when catalog is specified separately
      catalog: "${var.catalog}"  # CRITICAL: Required for serverless compute
      serverless: true  # MANDATORY: Must be true - NO cluster configurations allowed
      
      libraries:
        # CRITICAL: Reference individual files, NOT directories
        # Shared schemas
        - file:
            path: ../<relative_path>/shared/healthcare_schemas.py
        # Bronze layer ingestion
        - file:
            path: ../<relative_path>/bronze/patient_demographics_ingestion.py
        - file:
            path: ../<relative_path>/bronze/insurance_claims_ingestion.py
        - file:
            path: ../<relative_path>/bronze/medical_events_ingestion.py
        # Silver layer transformations
        - file:
            path: ../<relative_path>/silver/patient_demographics_transform.py
        - file:
            path: ../<relative_path>/silver/insurance_claims_transform.py
        # Gold layer dimensions
        - file:
            path: ../<relative_path>/gold/patient_360_dimension.py
            
      configuration:
        "CATALOG": "${var.catalog}"
        "SCHEMA": "${var.schema}"
        "PIPELINE_ENV": "${var.environment}"
        "VOLUMES_PATH": "${var.volumes_path}"
        "MAX_FILES_PER_TRIGGER": "${var.max_files_per_trigger}"
        # Healthcare compliance configurations (serverless handles compute automatically)
        # "spark.databricks.delta.properties.defaults.encryption.enabled": "true"
        "spark.databricks.delta.properties.defaults.changeDataFeed.enabled": "true"
        # Compliance and governance metadata (stored as configuration parameters)
        "compliance": "HIPAA"
        "data_classification": "PHI"
        "environment": "${var.environment}"
      # Continuous processing for healthcare workloads  
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

#### Pipeline Configuration Pattern
```python
# Environment-aware configuration loading - CRITICAL pattern for all pipeline files
CATALOG = spark.conf.get("CATALOG", "{catalog_name}")
SCHEMA = spark.conf.get("SCHEMA", "{schema_name}")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "{sdlc_env}")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/{catalog_name}/{schema_name}/raw_data")
MAX_FILES_PER_TRIGGER = spark.conf.get("MAX_FILES_PER_TRIGGER", "100")
```

#### ❌ FORBIDDEN PATH HANDLING ANTI-PATTERNS (NEVER CREATE THESE)
```python
# ❌ NEVER: Complex path resolution code in DLT pipelines
# ❌ NEVER: dbutils.notebook.entry_point resolution attempts
# ❌ NEVER: sys.path manipulation in pipeline files
# ❌ NEVER: Multiple fallback path attempts with try/except blocks

# WRONG EXAMPLE - DO NOT USE:
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = "/".join(notebook_path.split("/")[:-3])
    sys.path.insert(0, f"{base_path}/src")
except Exception as e1:
    try:
        current_path = os.path.dirname(os.path.abspath(__file__))
        # ... more complex path handling
    except Exception as e2:
        # ... fallback paths
        pass

# ✅ CORRECT: Simple imports work in Databricks DLT
from shared.healthcare_schemas import HealthcareSchemas
from shared.healthcare_utils import PipelineUtilities
```

**Path Handling Rule**: DLT pipelines should use simple imports. Asset Bundle deployment ensures proper module resolution.

### DLT Pipeline Patterns

#### Critical Autoloader Pattern for DLT Streaming
**🚨 MANDATORY**: When using Autoloader (`cloudFiles`) with DLT streaming tables, you **MUST** include `.format("cloudFiles")`:

```python
# ✅ CORRECT - Required format for Autoloader with DLT
@dlt.table(name="bronze_table_raw")
def bronze_table_raw():
    return (
        spark.readStream.format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/table_name")
        .schema(TABLE_SCHEMA)
        .load(f"{VOLUMES_PATH}")
    )

# ❌ INCORRECT - Missing .format("cloudFiles") 
@dlt.table(name="bronze_table_raw")
def bronze_table_raw():
    return (
        spark.readStream  # ← MISSING: .format("cloudFiles")
        .option("cloudFiles.format", "csv")  # This alone is not sufficient
        .option("header", "true")
        .load(f"{VOLUMES_PATH}")
    )
```

**Why this matters**: DLT requires explicit `.format("cloudFiles")` to recognize and optimize Autoloader functionality. Missing this pattern causes pipeline failures and performance issues. Simply having `"cloudFiles.format"` option is not sufficient - you need both.

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

# Bronze layer with schema enforcement and cloud files
@dlt.table(
    name="bronze_events",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
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
    name="bronze_external_customers",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="External customer data via Lakeflow Connect"
)
def bronze_external_customers():
    return dlt.read_stream("lakeflow.salesforce.customers")

# Silver layer with business logic and enhanced data quality
@dlt.view(name="silver_events_staging")
def silver_events_staging():
    """Staging view for silver transformation"""
    return (
        dlt.read("bronze_events")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("_rescued_data IS NULL")  # Filter out malformed records
        .filter("event_timestamp >= current_date() - interval 7 days")  # Recent data only
    )

@dlt.table(
    name="silver_events",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
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
    name="gold_daily_user_metrics",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Daily user engagement metrics for analytics",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_daily_user_metrics():
    return (
        dlt.read("silver_events")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
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

### File Metadata Patterns

**CRITICAL**: Always use the `_metadata` column for file information instead of deprecated functions like `input_file_name()`.

#### Correct File Metadata Pattern
```python
# Correct approach using _metadata column
return (
    spark.readStream
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema)
    .load(f"{VOLUMES_PATH}/data_*.csv")
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

**IMPORTANT**: Aggregate checks must NOT be used inside expectations. Expectations must be row-level Boolean expressions only.

#### Core Expectation Types
- `@dlt.expect_all_or_drop()` - Drop rows that fail expectations (use for critical data quality)
- `@dlt.expect_all()` - Log violations but keep processing (use for monitoring)
- `@dlt.expect_or_drop()` - Drop individual failing rows (use for outlier removal)
- `@dlt.expect_or_fail()` - Fail entire pipeline on violations (use for schema validation)

#### Advanced Quality Patterns
**Note**: Do not use aggregate expressions like COUNT(), SUM(), or AVG() inside @dlt.expect_* decorators. Use a separate metrics table for aggregate validations.
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

#### 🚨 SERVERLESS COMPUTE ONLY (NO EXCEPTIONS)
1. **Serverless by Omission**: ALL jobs use serverless compute by NOT specifying cluster configurations
2. **Forbidden Sections**: NEVER create `job_clusters:`, `new_cluster:`, `existing_cluster_id:`, `clusters:`
3. **DLT Serverless**: Pipelines must specify `serverless: true` with NO cluster configurations
4. **No Task-Level Clusters**: Tasks must NOT include cluster references
5. **Forbidden Fields**: NEVER use `node_type_id`, `num_workers`, `spark_version`, `driver_node_type_id`

#### Asset Bundle Requirements
6. **Asset Bundle First**: Always use `databricks bundle deploy` - never deploy manually
7. **Environment Isolation**: Use Asset Bundle targets for dev/staging/prod separation
8. **Version Control**: All databricks.yml configurations must be version controlled
9. **Configuration via Variables**: Use Asset Bundle variables with `spark.conf.get()` pattern
10. **Include Pattern**: Use `include: - resources/*.yml` for resource organization
11. **Variable Defaults**: Always provide sensible defaults for Asset Bundle variables

#### Pipeline Development Standards
12. **DLT Table Names**: Use simple table names in `@dlt.table(name="table_name")` - NO full namespace
13. **DLT Table References**: Use simple table names in `dlt.read("table_name")` - NO full namespace
14. **Unity Catalog**: Catalog and schema specified at pipeline level in Asset Bundle
15. **Configuration Variables**: Use `spark.conf.get("CATALOG", "default")` with Asset Bundle variables
16. **DLT Dependencies**: Use `dlt.read()` and `dlt.read_stream()` for dependencies, never `spark.read()`
17. **Data Quality**: Include multi-level `@dlt.expect_*` decorators on all tables
18. **Schema Enforcement**: Define explicit schemas for bronze layer ingestion
19. **Simple Imports**: Use standard Python imports - NO complex path resolution needed
20. **PII Handling**: Mark PII fields in table properties for governance compliance
21. **Change Data Capture**: Enable CDC on gold tables with `delta.enableChangeDataFeed`

### 🚨 Critical DLT Best Practices

1. **Streaming Ingestion**: Always use `@dlt.table` (not `@dlt.view`) for raw ingestion from Autoloader
   - `@dlt.table` ensures streaming tables are persisted, checkpointed, and visible to downstream pipelines
   - `@dlt.view` creates non-materialized logic that may cause "Dataset not defined" errors

2. **Autoloader Format**: When using `cloudFiles` with DLT streaming, you MUST include `.format("cloudFiles")`
   - Missing this pattern causes pipeline failures and performance issues

### Common Pitfalls to Avoid

**🚨 CRITICAL REMINDERS**
- **SERVERLESS ONLY**: Never create cluster configurations - all pipelines/jobs use serverless compute
- **NO AGGREGATE EXPECTATIONS**: Move aggregate checks to separate metrics tables
- **NO COMPLEX PATH HANDLING**: DLT pipelines use simple imports - never add dbutils path resolution code

#### Asset Bundle Configuration Pitfalls
- **Using cluster configurations** - NEVER create job_clusters, new_cluster, or clusters sections (MUST use serverless only)
- **Manual deployment** instead of using Asset Bundles
- **Missing environment separation** in Asset Bundle configuration
- **Hard-coding workspace URLs** or environment-specific values in databricks.yml
- **Creating unused utility modules** - prefer Asset Bundle variables + `spark.conf.get()` pattern
- **Adding complex path handling** in pipeline files - DLT pipelines use simple imports, no path resolution needed
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

## Key Files

- `PRPs/prp_base.md` - Template for systematic feature planning
- `databricks.yml` - Asset Bundle configuration (to be created)
- `resources/pipelines.yml` - Pipeline resource definitions (to be created)

- Pipeline files should return DataFrames and use DLT decorators properly

This project emphasizes modern data engineering practices using Databricks platform capabilities with enterprise-grade deployment management through Asset Bundles. 
