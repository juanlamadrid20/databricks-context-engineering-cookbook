# CLAUDE.md - Medallion Architecture Foundation Template

**🚨 IMPORTANT: This is a GENERIC FOUNDATION TEMPLATE for Databricks medallion architecture projects. You MUST customize it for your specific domain and business entities.**

## How to Use This Foundation Template

### Template Customization Steps

1. **Replace placeholder values** throughout this file with your domain-specific values:
   - `{DOMAIN}` → your business domain (e.g., healthcare, retail, finance)
   - `{ENTITY_1}`, `{ENTITY_2}`, `{ENTITY_3}` → your business entities
   - `{PROJECT_NAME}` → your actual project name
   - `{COMPLIANCE_FRAMEWORK}` → your regulatory requirements

2. **Update examples and schemas** to match your actual data structures
3. **Customize validation rules** for your business domain
4. **Add domain-specific gotchas** and best practices

---

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks data engineering project template implementing a medallion architecture (Bronze → Silver → Gold) using Declarative Pipelines (Delta Live Tables/DLT) and Databricks Asset Bundles for deployment management. This project follows a **PRP (Product Requirement Prompt) Framework**, not a traditional software project. The core concept: **"PRP = PRD + curated codebase intelligence + agent/runbook"** - designed to enable AI agents to ship production-ready code on the first pass.

### AI Documentation Curation

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
2. **ULTRATHINK**: Create comprehensive plan, break down into todos, use subagents, batch tool etc
3. **Execute**: Implement following the blueprint
4. **Validate**: Run each validation command, fix failures
5. **Complete**: Ensure all checklist items done

## Documentation & References

```yaml
# MUST READ - Include these in your context window
- url: https://docs.databricks.com
  why: "Databricks Specific Features and Documentation"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
  why: "Lakeflow Declarative Pipeline Syntax, decorators (@dlt.table, @dlt.expect_*), streaming tables, and pipeline patterns"

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

# TODO: Add domain-specific documentation URLs relevant to your business context
# Examples:
# - url: {DOMAIN_SPECIFIC_STANDARDS_URL}
#   why: "{DOMAIN} data standards specifications for {DOMAIN} validation rules and industry interoperability requirements"
# - url: {COMPLIANCE_FRAMEWORK_URL}
#   why: "{COMPLIANCE_FRAMEWORK} compliance requirements for data handling, encryption, audit logging, and access controls"
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

# TODO: Add domain-specific commands relevant to your business context
# Examples:
# custom-data-validation-script.py         # Custom validation for your domain
# domain-specific-quality-checks.py        # Business rule validation
```

## Architecture Overview

### Project Structure
- `PRPs/` - Problem Requirements & Proposals for planning features
- `PRPs/templates/` - PRP templates for systematic feature development
- `src/` - Source code (to be created during development)
  - `src/data_generation/` - Synthetic data generation jobs (TODO: customize for {domain} data generation)
  - `src/pipelines/` - Delta Live Tables pipeline definitions
    - `src/pipelines/bronze/` - Raw data ingestion (TODO: customize for {entity_1}, {entity_2}, {entity_3})
    - `src/pipelines/silver/` - Data cleaning and transformation (TODO: add {domain}-specific transformations)
    - `src/pipelines/gold/` - Business-ready dimensional models (TODO: customize for {domain} analytics)
  - `src/tests/` - Unit and integration tests (TODO: add {domain}-specific test cases)

### Data Engineering Patterns
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Delta Live Tables**: Declarative pipeline definitions with data quality expectations
- **Asset Bundle Management**: Infrastructure-as-code for all deployments
- **Environment Isolation**: Separate dev/staging/prod configurations

## Critical Development Patterns

### 🚨 MANDATORY: Environment-Aware Configuration Loading

**CRITICAL**: This pattern must be used in ALL pipeline files:

```python
# Environment-aware configuration loading pattern for ALL pipeline files
CATALOG = spark.conf.get("CATALOG", "{catalog_name}")
SCHEMA = spark.conf.get("SCHEMA", "{schema_name}")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "{sdlc_env}")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/{catalog_name}/{schema_name}/raw_data")
MAX_FILES_PER_TRIGGER = spark.conf.get("MAX_FILES_PER_TRIGGER", "100")

# TODO: Add domain-specific configuration parameters
# COMPLIANCE_FRAMEWORK = spark.conf.get("COMPLIANCE", "{compliance_framework}")
# DATA_CLASSIFICATION = spark.conf.get("DATA_CLASSIFICATION", "{data_classification}")
```

### 🚨 MANDATORY: Autoloader with DLT Streaming

**CRITICAL**: When using Autoloader (`cloudFiles`) with DLT streaming tables, you **MUST** include `.format("cloudFiles")`:

```python
# CRITICAL: Autoloader with DLT streaming - MUST include .format("cloudFiles")
@dlt.table(name="bronze_table_raw")
def bronze_table_raw():
    return (
        spark.readStream.format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/table_name")
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(TABLE_SCHEMA)
        .load(f"{VOLUMES_PATH}")
        # CRITICAL: File metadata pattern (NOT deprecated input_file_name())
        .select("*", "_metadata")  # REQUIRED: Explicitly select metadata
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
        .drop("_metadata")  # Clean up temporary column
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )
```

**Why this matters**: DLT requires explicit `.format("cloudFiles")` to recognize and optimize Autoloader functionality. Missing this pattern causes pipeline failures and performance issues.

### Asset Bundle Management - Single Source of Truth

#### Comprehensive Root `databricks.yml` Template

```yaml
bundle:
  name: <TODO: your-project-name>-pipeline  # TODO: Replace with your project name
variables:
  catalog:
    description: "Unity Catalog name"
  schema:
    description: "Schema name within catalog"
    default: "<TODO: your_default_schema>"  # TODO: Replace with your schema name
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
      catalog: "{catalog_name}"  # TODO: Replace with your catalog
      schema: "{schema_name}"    # TODO: Replace with your schema
      environment: "dev"
      volumes_path: "/Volumes/{catalog_name}/{schema_name}/raw_data"
      max_files_per_trigger: 50
      
  prod:
    mode: production
    variables:
      catalog: "{catalog_name}"  # TODO: Replace with your catalog
      schema: "{schema_name}"    # TODO: Replace with your schema
      environment: "prod"
      volumes_path: "/Volumes/{catalog_name}/{schema_name}/raw_data"
      max_files_per_trigger: 200
```

#### DLT Pipeline Resource Configuration (SERVERLESS ONLY)
```yaml
# resources/pipelines.yml
resources:
  pipelines:
    <TODO: your_pipeline_name>:  # TODO: Replace with your pipeline name
      name: "<TODO: your-project-name>-pipeline-${var.environment}"  # TODO: Replace with your project name
      target: "${var.schema}"  # CRITICAL: Only schema when catalog is specified separately
      catalog: "${var.catalog}"  # CRITICAL: Required for serverless compute
      serverless: true  # MANDATORY: Must be true - NO cluster configurations allowed
      
      libraries:
        # CRITICAL: Reference individual files, NOT directories
        # TODO: Update paths to match your business entities
        # Shared schemas
        - file:
            path: ../<relative_path>/shared/<TODO: your_domain>_schemas.py
        # Bronze layer ingestion
        - file:
            path: ../<relative_path>/bronze/<TODO: entity1>_ingestion.py
        - file:
            path: ../<relative_path>/bronze/<TODO: entity2>_ingestion.py
        - file:
            path: ../<relative_path>/bronze/<TODO: entity3>_ingestion.py
        # Silver layer transformations  
        - file:
            path: ../<relative_path>/silver/<TODO: entity1>_transform.py
        - file:
            path: ../<relative_path>/silver/<TODO: entity2>_transform.py
        # Gold layer dimensions
        - file:
            path: ../<relative_path>/gold/<TODO: entity1>_dimension.py
            
      configuration:
        "CATALOG": "${var.catalog}"
        "SCHEMA": "${var.schema}"
        "PIPELINE_ENV": "${var.environment}"
        "VOLUMES_PATH": "${var.volumes_path}"
        "MAX_FILES_PER_TRIGGER": "${var.max_files_per_trigger}"
        # TODO: Add domain-specific configurations
        # "DATA_CLASSIFICATION": "<TODO: your_data_classification>"
        # "COMPLIANCE_REQUIREMENTS": "<TODO: your_compliance_requirements>"
        "spark.databricks.delta.properties.defaults.changeDataFeed.enabled": "true"
        # TODO: Compliance and governance metadata (stored as configuration parameters)
        "compliance": "<TODO: your_compliance_framework>"  # TODO: Replace with your compliance framework
        "data_classification": "<TODO: your_data_classification>"  # TODO: Replace with your data classification
        "environment": "${var.environment}"
      # TODO: Continuous processing based on business requirements  
      continuous: false
      
      # CRITICAL: Additional pipeline configuration patterns
      edition: "ADVANCED"  # Required for serverless compute
      # TODO: Add development mode for faster iteration in dev environments
      # development: true  # Only for dev/staging - not production
```

#### Serverless Job Configuration Pattern (CORRECT - NO CLUSTER CONFIGS)
```yaml
# resources/jobs.yml - CORRECT SERVERLESS PATTERN (default when no cluster specified)
resources:
  jobs:
    <TODO: your_job_name>:  # TODO: Replace with your job name
      name: "<TODO: job-name>-${var.environment}"  # TODO: Replace with your job name
      # ✅ CORRECT: NO cluster configuration = serverless by default
      tasks:
        - task_key: "<TODO: task_name>"  # TODO: Replace with your task name
          # ✅ CORRECT: No compute_key or cluster references
          notebook_task:
            notebook_path: "../src/path/to/notebook"  # TODO: Update path
            base_parameters:
              catalog: "${var.catalog}"
              schema: "${var.schema}"
        - task_key: "<TODO: another_task>"  # TODO: Replace with your task name
          python_wheel_task:
            package_name: "<TODO: my_package>"  # TODO: Replace with your package
            entry_point: "<TODO: main_function>"  # TODO: Replace with your entry point
            parameters:
              - "--param=value"  # TODO: Update parameters for your domain
```

### Complete DLT Pipeline Template
```python
import dlt
import sys
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Environment-aware configuration with Unity Catalog
CATALOG = spark.conf.get("bundle.target.catalog", "default_catalog")
SCHEMA = spark.conf.get("bundle.target.schema", "default_schema")
PIPELINE_ENV = spark.conf.get("bundle.target.name", "dev")

# Bronze layer with schema enforcement and cloud files (TODO: Customize for your entity)
@dlt.table(
    name="bronze_<TODO: entity>",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Raw <TODO: entity> data ingestion with schema enforcement",  # TODO: Update comment for your entity
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported"
    }
)
@dlt.expect_all_or_drop({
    "valid_<TODO: entity>_id": "<TODO: entity>_id IS NOT NULL AND LENGTH(<TODO: entity>_id) > 0",  # TODO: Update validation for your entity
    "valid_timestamp": "<TODO: timestamp_field> IS NOT NULL"  # TODO: Update timestamp field name
})
def bronze_<TODO: entity>():  # TODO: Replace with your entity name
    schema = StructType([
        StructField("<TODO: entity>_id", StringType(), False),    # TODO: Replace with your entity ID field
        StructField("<TODO: field1>", StringType(), True),        # TODO: Replace with your actual fields
        StructField("<TODO: field2>", StringType(), True),        # TODO: Replace with your actual fields
        StructField("<TODO: timestamp_field>", TimestampType(), False),  # TODO: Replace with your timestamp field
        StructField("<TODO: field3>", StringType(), True)         # TODO: Add your specific fields here
    ])
    
    return (
        spark.readStream
        .option("cloudFiles.format", "<TODO: json|csv|parquet>")  # TODO: Update format for your data
        .option("cloudFiles.schemaLocation", f"/tmp/schemas/{PIPELINE_ENV}/<TODO: entity>")  # TODO: Update entity name
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(spark.conf.get("source.<TODO: entity>.path", "/mnt/data/<TODO: entity>/"))  # TODO: Update source path
        .select("*", "_metadata")  # Include file metadata
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
        .drop("_metadata")  # Clean up after extracting needed fields
    )

# Lakeflow Connect pattern for external data sources (TODO: Customize for your external source)
@dlt.table(
    name="bronze_external_<TODO: entity>",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="External <TODO: entity> data via Lakeflow Connect"  # TODO: Update for your external source
)
def bronze_external_<TODO: entity>():  # TODO: Replace with your entity name
    return dlt.read_stream("lakeflow.<TODO: source>.<TODO: entity>")  # TODO: Update with your external source

# Silver layer with business logic and enhanced data quality (TODO: Customize for your entity)
@dlt.view(name="silver_<TODO: entity>_staging")  # TODO: Replace with your entity name
def silver_<TODO: entity>_staging():  # TODO: Replace with your entity name
    """Staging view for silver transformation"""
    return (
        dlt.read("bronze_<TODO: entity>")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("_rescued_data IS NULL")  # Filter out malformed records
        .filter("<TODO: timestamp_field> >= current_date() - interval <TODO: days> days")  # TODO: Update filter for your data
    )

@dlt.table(
    name="silver_<TODO: entity>",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Cleaned and enriched <TODO: entity> data",  # TODO: Update comment for your entity
    table_properties={
        "quality": "silver",
        # TODO: Add PII fields if applicable
        # "pipelines.pii.fields": "<TODO: pii_field1,pii_field2>"
    }
)
@dlt.expect_all_or_drop({
    "valid_<TODO: entity>_id": "<TODO: entity>_id IS NOT NULL",  # TODO: Update validation for your entity
    # TODO: Add your business-specific validation rules
    "valid_<TODO: field>": "<TODO: field> IS NOT NULL OR <TODO: condition>",
    "future_timestamp": "<TODO: timestamp_field> <= current_timestamp()"
})
@dlt.expect_or_fail({
    "no_duplicates": "COUNT(*) = COUNT(DISTINCT <TODO: entity>_id)"  # TODO: Update for your entity
})
def silver_<TODO: entity>():  # TODO: Replace with your entity name
    return (
        dlt.read("silver_<TODO: entity>_staging")  # TODO: Replace with your entity name
        .withColumn("<TODO: derived_field>", col("<TODO: source_field>").cast("<TODO: target_type>"))  # TODO: Add your transformations
        # TODO: Add your business transformations here
        .withColumn("processed_at", current_timestamp())
    )

# Gold layer aggregations with SCD Type 2 support (TODO: Customize for your analytics)
@dlt.table(
    name="gold_<TODO: entity>_metrics",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="<TODO: Description of gold layer purpose>",  # TODO: Update description
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_<TODO: entity>_metrics():  # TODO: Replace with your entity name
    return (
        dlt.read("silver_<TODO: entity>")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy("<TODO: grouping_field1>", "<TODO: grouping_field2>")  # TODO: Update grouping fields
        .agg(
            count("*").alias("total_<TODO: entity>"),  # TODO: Update metric names
            countDistinct("<TODO: field>").alias("unique_<TODO: field>"),  # TODO: Update fields
            # TODO: Add your specific aggregations
        )
        # TODO: Add your business logic for derived columns
    )
```

### Data Quality Expectations & Monitoring

**IMPORTANT**: Aggregate checks must NOT be used inside expectations. Expectations must be row-level Boolean expressions only.

#### Core Expectation Types
- `@dlt.expect_all_or_drop()` - Drop rows that fail expectations (use for critical data quality)
- `@dlt.expect_all()` - Log violations but keep processing (use for monitoring)
- `@dlt.expect_or_drop()` - Drop individual failing rows (use for outlier removal)
- `@dlt.expect_or_fail()` - Fail entire pipeline on violations (use for schema validation)

#### Advanced Quality Patterns
```python
# Multi-level data quality with custom metrics (TODO: Customize for your entity)
@dlt.table(name="silver_<TODO: entity>")  # TODO: Replace with your entity
@dlt.expect_all_or_drop({
    "valid_<TODO: entity>_id": "<TODO: entity>_id IS NOT NULL AND LENGTH(<TODO: entity>_id) >= <TODO: min_length>",  # TODO: Update for your entity
    # TODO: Add your domain-specific validation rules
    "valid_<TODO: field1>": "<TODO: field1> <TODO: validation_condition>",
    "valid_<TODO: date_field>": "<TODO: date_field> <= current_date()"
})
@dlt.expect_all({
    # TODO: Add monitoring expectations (non-critical)
    "reasonable_<TODO: field>": "<TODO: field> BETWEEN <TODO: min_value> AND <TODO: max_value>",
    "complete_<TODO: profile>": "<TODO: required_fields> IS NOT NULL"
})
@dlt.expect_or_fail({
    "no_duplicate_<TODO: key>": "COUNT(*) = COUNT(DISTINCT <TODO: key_field>)",
    "schema_compliance": "_rescued_data IS NULL"
})
def silver_<TODO: entity>():  # TODO: Replace with your entity
    return dlt.read("bronze_<TODO: entity>")  # TODO: Replace with your entity
```

## Development Guidelines

### 🚨 MANDATORY: Serverless Compute Only (NO EXCEPTIONS)

1. **Serverless by Omission**: ALL jobs use serverless compute by NOT specifying cluster configurations
2. **Forbidden Sections**: NEVER create `job_clusters:`, `new_cluster:`, `existing_cluster_id:`, `clusters:`
3. **DLT Serverless**: Pipelines must specify `serverless: true` with NO cluster configurations
4. **No Task-Level Clusters**: Tasks must NOT include cluster references
5. **Forbidden Fields**: NEVER use `node_type_id`, `num_workers`, `spark_version`, `driver_node_type_id`

### Asset Bundle Requirements
6. **Asset Bundle First**: Always use `databricks bundle deploy` - never deploy manually
7. **Environment Isolation**: Use Asset Bundle targets for dev/staging/prod separation
8. **Version Control**: All databricks.yml configurations must be version controlled
9. **Configuration via Variables**: Use Asset Bundle variables with `spark.conf.get()` pattern
10. **Include Pattern**: Use `include: - resources/*.yml` for resource organization
11. **Variable Defaults**: Always provide sensible defaults for Asset Bundle variables

### Pipeline Development Standards
12. **DLT Table Names**: Use simple table names in `@dlt.table(name="table_name")` - NO full namespace
13. **DLT Table References**: Use simple table names in `dlt.read("table_name")` - NO full namespace
14. **Unity Catalog**: Catalog and schema specified at pipeline level in Asset Bundle
15. **Configuration Variables**: Use `spark.conf.get("CATALOG", "default")` with Asset Bundle variables
16. **DLT Dependencies**: Use `dlt.read()` and `dlt.read_stream()` for dependencies, never `spark.read()`
17. **Data Quality**: Include multi-level `@dlt.expect_*` decorators on all tables
18. **Schema Enforcement**: Define explicit schemas for bronze layer ingestion
19. **Simple Imports**: Use standard Python imports - NO complex path resolution needed
20. **PII Handling**: Mark PII fields in table properties for governance compliance (TODO: customize for your domain)
21. **Change Data Capture**: Enable CDC on gold tables with `delta.enableChangeDataFeed`

### 🚨 Critical DLT Best Practices

1. **Streaming Ingestion**: Always use `@dlt.table` (not `@dlt.view`) for raw ingestion from Autoloader
   - `@dlt.table` ensures streaming tables are persisted, checkpointed, and visible to downstream pipelines
   - `@dlt.view` creates non-materialized logic that may cause "Dataset not defined" errors

2. **Autoloader Format**: When using `cloudFiles` with DLT streaming, you MUST include `.format("cloudFiles")`
   - Missing this pattern causes pipeline failures and performance issues

### ❌ FORBIDDEN PATTERNS (NEVER CREATE THESE)

#### Asset Bundle Anti-Patterns
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

#### Path Handling Anti-Patterns
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
from shared.<TODO: your_domain>_schemas import <TODO: YourDomainSchemas>  # TODO: Replace with your domain schemas
from shared.<TODO: your_domain>_utils import <TODO: PipelineUtilities>    # TODO: Replace with your utilities
```

#### DLT Anti-Patterns
```python
# ❌ NEVER: Using input_file_name() - it's deprecated
from pyspark.sql.functions import input_file_name
.withColumn("_file_name", input_file_name())  # This is deprecated

# ❌ NEVER: Using full namespace in dlt.read() calls
dlt.read(f"{CATALOG}.{SCHEMA}.table_name")  # WRONG - breaks DLT lineage

# ❌ NEVER: Using spark.read() for DLT dependencies
spark.read.table("table_name")  # WRONG - breaks DLT lineage

# ❌ NEVER: Using display() in DLT functions
@dlt.table()
def my_table():
    df = dlt.read("source_table")
    display(df)  # WRONG - return DataFrames instead
    
# ❌ NEVER: Using aggregate expressions in @dlt.expect_*
@dlt.expect_all({
    "count_check": "COUNT(*) > 100"  # WRONG - expectations must be row-level
})

# ✅ CORRECT: Row-level expectations only
@dlt.expect_all({
    "valid_id": "id IS NOT NULL"  # CORRECT - row-level Boolean expression
})
```

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
8. **TODO: Domain-Specific Testing**: Add business-specific validation tests

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

# TODO: Add domain-specific validation commands
# Examples:
# custom-business-rule-validation.py
# domain-specific-data-quality-checks.py
```

## Known Gotchas of Codebase & Databricks Quirks
```python
# CRITICAL: Delta Live Tables requires specific decorators and patterns
# Example: @dlt.table() functions must return DataFrames, not display()
# Example: Asset Bundles use specific variable interpolation syntax ${var.environment}
# Example: Pipeline dependencies must be explicit using dlt.read() or dlt.read_stream()

# TODO: DOMAIN-SPECIFIC GOTCHAS - ADD YOUR BUSINESS CONTEXT GOTCHAS HERE
# CRITICAL: <TODO: Add critical gotchas specific to your business domain>
# CRITICAL: <TODO: Add data handling requirements specific to your industry>
# CRITICAL: <TODO: Add compliance requirements specific to your use case>
# CRITICAL: <TODO: Add temporal requirements specific to your business logic>
# CRITICAL: <TODO: Add validation requirements specific to your data sources>
# CRITICAL: <TODO: Add audit requirements specific to your governance needs>
```

## Additional Instructions

# TODO: Reference domain-specific CLAUDE files for guidance when working with your business context
# Reference @data-eng/examples/<TODO: your-domain>/claude_md_files/CLAUDE-PYTHON-BASIC.md for guidance when working with Python code in this repository.
