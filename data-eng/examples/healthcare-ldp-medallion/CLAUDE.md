# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks data engineering project template implementing a medallion architecture (Bronze → Silver → Gold) using Declarative Pipelines (Delta Live Tables/DLT) and Databricks Asset Bundles for deployment management.

### Core Principles
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

### Command-Driven System

- **pre-configured Claude Code commands** in `.claude/commands/`
- Commands organized by function:
  - `PRPs/` - PRP creation and execution workflows
  - `development/` - Core development utilities (prime-core, onboarding, debug)
  - `code-quality/` - Review and refactoring commands
  - `git-operations/` - Conflict resolution and smart git operations

### AI Documentation Curation

- `PRPs/ai_docs/` contains curated Claude Code documentation for context injection
- `claude_md_files/` provides framework-specific CLAUDE.md examples

### Key Claude Commands

- `/prp-base-create` - Generate comprehensive PRPs with research
- `/prp-base-execute` - Execute PRPs against codebase
- `/prime-core` - Prime Claude with project context

### Documentation & References

```yaml
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

### Databricks Lakeflow Declarative Pipelines (LDP)

You are helping develop **Lakeflow Declarative Pipelines** using Databricks best practices. Focus on:

#### Core LDP Components
- **Pipelines**: Complete data processing workflows
- **Flows**: Individual pipeline components 
- **Streaming Tables**: Real-time data processing tables
- **Materialized Views**: Optimized query result storage

#### Primary Use Cases
1. **Data Ingestion**
   - Cloud storage: S3, ADLS Gen2, Google Cloud Storage
   - Message buses: Kafka, Kinesis, Pub/Sub, EventHub, Pulsar
2. **Data Transformations**
   - Incremental batch processing
   - Real-time streaming processing

#### Development Languages
- **Python**: For complex transformations and custom logic

### LDP Best Practices

#### Pipeline Development
1. **Declarative Approach**: Use SQL DDL statements to define data transformations
2. **Incremental Processing**: Implement incremental batch and streaming patterns
3. **Data Quality**: Include data quality constraints and expectations
4. **Error Handling**: Implement robust error handling and retry logic
5. **Monitoring**: Use built-in monitoring and alerting features

#### Code Organization
- Organize pipelines by business domain or data source
- Use clear, descriptive naming conventions
- Document pipeline dependencies and data lineage
- Implement proper version control practices

#### Performance Optimization
- Use appropriate clustering and partitioning strategies
- Optimize for streaming vs batch processing patterns
- Monitor pipeline performance metrics
- Implement proper resource allocation


### Synthetic Data Generation with dbldatagen

When working with **dbldatagen** for generating synthetic test data at scale:

#### Core Components
- **DataGenerator** - Primary entry point for data generation
- **ColumnGenerationSpec** - Defines how individual columns should be generated
- **Constraints System** - Ensures data relationships and validity rules
- **Distributions** - Support for various statistical distributions (Normal, Beta, Gamma, Exponential)
- **Text Generation** - Template-based and Faker-integrated text generation
- **Standard Datasets** - Pre-built realistic datasets for common use cases

#### Integration with LDP
- Use dbldatagen to create realistic test data for pipeline development
- Generate data that matches production schemas and constraints
- Create streaming data sources for real-time pipeline testing
- Build comprehensive test datasets for data quality validation

#### Key Patterns
```python
# Basic data generation for LDP testing
from dbldatagen import DataGenerator
import dbldatagen as dg

# Generate user events for streaming pipeline
df_spec = (DataGenerator(spark, rows=1000000, partitions=8)
           .withColumn("user_id", "long", minValue=1000000, maxValue=9999999)
           .withColumn("event_time", "timestamp", begin="2024-01-01", end="2024-12-31")
           .withColumn("event_type", "string", values=["click", "view", "purchase"])
           .withColumn("amount", "decimal(10,2)", minValue=0.01, maxValue=1000.00)
)

df = df_spec.build()
```
