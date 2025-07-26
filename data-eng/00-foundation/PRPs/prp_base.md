name: "Databricks PRP Template v2 - Context-Rich with Validation Loops"
description: |

## Purpose
Template optimized for AI agents to implement Databricks features with sufficient context and self-validation capabilities to achieve working Delta Live Tables, Declarative Pipelines, and Asset Bundle configurations through iterative refinement.

## Core Principles
1. **Context is King**: Include ALL necessary documentation, examples, and caveats
2. **Validation Loops**: Provide executable tests/lints the AI can run and fix
3. **Information Dense**: Use keywords and patterns from the Databricks ecosystem
4. **Progressive Success**: Start simple, validate, then enhance
5. **Global rules**: Be sure to follow all rules in CLAUDE.md
6. **Asset Bundle First**: All infrastructure should be managed through Databricks Asset Bundles

---

## Goal
**<TODO: What needs to be built - be specific about the end state and desires for Delta Live Tables, pipelines, or data workflows>**

## Why
- **<TODO: Business value and data impact>**
- **<TODO: Integration with existing data pipelines>**
- **<TODO: Data quality problems this solves and for whom>**

## What
**<TODO: User-visible behavior and technical requirements for data pipeline/workflow>**

### Success Criteria
- [ ] **<TODO: Specific measurable outcomes - data quality, pipeline performance, etc.>**
- [ ] **<TODO: Additional success metrics relevant to your business domain>**

## All Needed Context

### Context Engineering Specifications
```yaml
# Context Definition & Scope
context_domain: **<TODO: Define your context domain (customer, product, behavioral, temporal, etc.)>**
context_sources:
  - source: **<TODO: External context providers (APIs, CRM, event streams, etc.)>**
    type: **<TODO: real-time/batch/streaming>**
    refresh_pattern: **<TODO: How often context updates (continuous/hourly/daily)>**
    latency_requirements: **<TODO: Maximum acceptable context staleness>**
    
context_resolution:
  - entity_type: **<TODO: Primary entities requiring context (users, products, events)>**
    resolution_keys: **<TODO: Keys used for context matching (ID, email, session, etc.)>**
    conflict_strategy: **<TODO: How to handle conflicting context (latest_wins/highest_confidence/merge)>**
    temporal_strategy: **<TODO: How to handle temporal context (point_in_time/sliding_window/cumulative)>**
    
context_quality_requirements:
  - completeness_threshold: **<TODO: Minimum context coverage % (e.g., 95%)>**
  - freshness_sla: **<TODO: Maximum context age allowed (e.g., 1 hour, 1 day)>**
  - consistency_rules: **<TODO: Cross-context validation rules>**
  - confidence_thresholds: **<TODO: Minimum confidence scores for context usage>**

context_graph_structure:
  - relationship_types: **<TODO: Types of context relationships (hierarchical/associative/temporal)>**
  - traversal_depth: **<TODO: Maximum relationship hops for enrichment>**
  - graph_algorithms: **<TODO: Graph algorithms needed (shortest_path/centrality/clustering)>**
```

### Documentation & References (list all context needed to implement the feature)
```yaml
# MUST READ - Include these in your context window
- url: **<TODO: Your business domain documentation URLs>**
  why: **<TODO: Specific business logic/rules you'll need to implement>**
  
- url: [Databricks Delta Live Tables docs URL]
  why: **<TODO: Specific DLT patterns/decorators you'll need>**
  
- file: **<TODO: path/to/existing_pipeline.py>**
  why: **<TODO: Pattern to follow, gotchas to avoid>**
  
- doc: [Databricks Asset Bundles documentation URL] 
  section: **<TODO: Specific section about deployment patterns>**
  critical: **<TODO: Key insight that prevents common deployment errors>**

- docfile: **<TODO: PRPs/ai_docs/your_domain_file.md>**
  why: **<TODO: Domain-specific documentation that the user has provided>**

```

### Current Codebase tree (run `tree` in the root of the project) to get an overview of the codebase
```bash
**<TODO: Paste the output of `tree` command from your project root>**
```

### Desired Codebase tree with files to be added and responsibility of file
```bash
# Example structure - customize for your project:
# src/
#   pipelines/
#     bronze/
#       **<TODO: your_raw_data_ingestion.py>**
#     silver/
#       **<TODO: your_data_transformation.py>**
#     gold/
#       **<TODO: your_aggregated_metrics.py>**
#   data_generation/
#     **<TODO: your_synthetic_data_generator.py>**
# databricks.yml (Asset Bundle configuration)
# resources/
#   pipelines.yml
#   workflows.yml
# tests/
#   **<TODO: your_unit_tests.py>**
```

### Known Gotchas of our codebase & Databricks Quirks
```python
# CRITICAL: Delta Live Tables requires specific decorators and patterns
# Example: @dlt.table() functions must return DataFrames, not display()
# Example: Asset Bundles use specific variable interpolation syntax ${var.environment}
# Example: Pipeline dependencies must be explicit using dlt.read() or dlt.read_stream()

# **<TODO: Add specific gotchas for your business domain>**
# **<TODO: Document any custom utility functions or patterns in your codebase>**
# **<TODO: Include any specific data source connection patterns>**
```

## Implementation Blueprint

### Context-Aware Data Models and Structure

Create context-aware data models ensuring proper relationship modeling, temporal handling, and Delta Lake compatibility.

```python
# Core Context Engineering Schemas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, MapType, ArrayType, DoubleType

# Primary Context Schema - Temporal context with versioning
CONTEXT_SCHEMA = StructType([
    StructField("context_id", StringType(), False),           # Unique context identifier
    StructField("entity_id", StringType(), False),            # Entity being contextualized
    StructField("entity_type", StringType(), False),          # Type of entity (user/product/session/etc)
    StructField("context_type", StringType(), False),         # Type of context (behavioral/demographic/transactional)
    StructField("context_data", MapType(StringType(), StringType()), True),  # Flexible context attributes
    StructField("confidence_score", DoubleType(), True),      # Context quality/confidence (0.0-1.0)
    StructField("effective_from", TimestampType(), False),    # When context becomes valid
    StructField("effective_to", TimestampType(), True),       # When context expires (NULL = current)
    StructField("source_system", StringType(), False),       # Origin system for lineage
    StructField("source_confidence", DoubleType(), True),    # Source reliability score
    StructField("version", IntegerType(), False),            # Version number for updates
    StructField("created_at", TimestampType(), False),       # Context creation timestamp
    StructField("updated_at", TimestampType(), True)         # Last update timestamp
])

# Context Relationship Schema - Graph structure for context interconnections
CONTEXT_RELATIONSHIP_SCHEMA = StructType([
    StructField("relationship_id", StringType(), False),     # Unique relationship identifier
    StructField("source_context_id", StringType(), False),  # Source context in relationship
    StructField("target_context_id", StringType(), False),  # Target context in relationship
    StructField("relationship_type", StringType(), False),  # Type (hierarchy/association/temporal/causal)
    StructField("strength", DoubleType(), True),            # Relationship strength (0.0-1.0)
    StructField("direction", StringType(), True),           # bidirectional/unidirectional
    StructField("created_at", TimestampType(), False),      # When relationship was established
    StructField("metadata", MapType(StringType(), StringType()), True)  # Additional relationship properties
])

# Context Resolution Schema - Entity resolution with context awareness
CONTEXT_RESOLUTION_SCHEMA = StructType([
    StructField("resolution_id", StringType(), False),      # Unique resolution identifier
    StructField("canonical_entity_id", StringType(), False), # Resolved canonical entity ID
    StructField("source_entity_id", StringType(), False),  # Original entity ID from source
    StructField("entity_type", StringType(), False),       # Entity type being resolved
    StructField("resolution_method", StringType(), False), # Algorithm used for resolution
    StructField("confidence_score", DoubleType(), False),  # Resolution confidence
    StructField("contributing_contexts", ArrayType(StringType()), True), # Context IDs used in resolution
    StructField("resolved_at", TimestampType(), False)     # When resolution occurred
])

# Context Quality Metrics Schema - Data quality tracking for context
CONTEXT_QUALITY_SCHEMA = StructType([
    StructField("quality_id", StringType(), False),        # Unique quality record ID
    StructField("context_id", StringType(), False),       # Context being measured
    StructField("completeness_score", DoubleType(), True), # % of expected attributes present
    StructField("freshness_score", DoubleType(), True),   # How recent the context is
    StructField("consistency_score", DoubleType(), True), # Consistency with other contexts
    StructField("validity_score", DoubleType(), True),    # Business rule validation score
    StructField("overall_quality_score", DoubleType(), False), # Composite quality score
    StructField("quality_dimensions", MapType(StringType(), DoubleType()), True), # Detailed quality metrics
    StructField("measured_at", TimestampType(), False)    # When quality was assessed
])

# **<TODO: Add your domain-specific context schemas>**
# Examples for common context domains:

# Customer Context Schema
CUSTOMER_CONTEXT_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("segment", StringType(), True),            # Customer segment
    StructField("lifetime_value", DoubleType(), True),     # CLV
    StructField("risk_score", DoubleType(), True),         # Churn/fraud risk
    StructField("preferences", MapType(StringType(), StringType()), True), # Preferences
    StructField("behavioral_traits", ArrayType(StringType()), True), # Behavioral patterns
    # Include base context fields from CONTEXT_SCHEMA
])

# Product Context Schema  
PRODUCT_CONTEXT_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("category_hierarchy", ArrayType(StringType()), True), # Category path
    StructField("popularity_score", DoubleType(), True),   # Trending/popularity metrics
    StructField("seasonal_patterns", MapType(StringType(), DoubleType()), True), # Seasonality
    StructField("cross_sell_products", ArrayType(StringType()), True), # Related products
    # Include base context fields from CONTEXT_SCHEMA
])

# Behavioral Context Schema
BEHAVIORAL_CONTEXT_SCHEMA = StructType([
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("behavior_type", StringType(), False),     # click/view/purchase/search
    StructField("intent_signals", ArrayType(StringType()), True), # Detected intents
    StructField("engagement_score", DoubleType(), True),   # Session engagement level
    StructField("context_sequence", IntegerType(), True),  # Order in behavior sequence
    # Include base context fields from CONTEXT_SCHEMA
])
```

### list of tasks to be completed to fullfill the PRP in the order they should be completed

```yaml
Task 1:
MODIFY databricks.yml:
  - ADD new pipeline resource for **<TODO: your_pipeline_name>**
  - CONFIGURE target environments (dev, staging, prod)
  - SET appropriate compute settings for **<TODO: your_workload_type>**

CREATE src/pipelines/bronze/**<TODO: your_source_name>.py**:
  - MIRROR pattern from: **<TODO: src/pipelines/bronze/existing_source.py>**
  - USE @dlt.table() or @dlt.streaming_table() decorators
  - IMPLEMENT data quality expectations for **<TODO: your_data_quality_rules>**

CREATE src/pipelines/silver/**<TODO: your_transformation_name>.py**:
  - IMPLEMENT business logic for **<TODO: your_transformation_requirements>**
  - ADD data quality expectations for **<TODO: your_validation_rules>**
  - USE proper dlt.read() dependencies

CREATE src/pipelines/gold/**<TODO: your_dimensional_model_name>.py**:
  - IMPLEMENT dimensional model for **<TODO: your_business_analytics>**
  - ADD aggregations for **<TODO: your_key_metrics>**
  - ENSURE proper SCD handling for **<TODO: your_dimension_tables>**

**<TODO: Add additional tasks specific to your implementation>**

Context Engineering Tasks:

Task Context-1:
CREATE src/pipelines/bronze/context_ingestion.py:
  - IMPLEMENT multi-source context ingestion for **<TODO: your_context_sources>**
  - ADD context deduplication and conflict detection logic
  - USE @dlt.expect_all_or_drop for context quality validation
  - HANDLE context versioning and temporal windows
  - IMPLEMENT schema evolution for changing context structures

Task Context-2:  
CREATE src/pipelines/silver/context_resolution.py:
  - IMPLEMENT entity resolution with context awareness
  - ADD conflict resolution logic for **<TODO: your_conflict_strategy>**
  - CALCULATE context confidence scores using **<TODO: your_scoring_algorithm>**
  - MAINTAIN context lineage and provenance tracking
  - HANDLE temporal context alignment and synchronization

Task Context-3:
CREATE src/pipelines/silver/context_enrichment.py:
  - IMPLEMENT context graph traversal for **<TODO: your_relationship_types>**
  - ADD temporal context joining with sliding windows
  - CALCULATE derived context attributes and computed metrics
  - ENSURE context freshness validation against SLA requirements
  - OPTIMIZE graph queries for performance at scale

Task Context-4:
CREATE src/pipelines/gold/context_analytics.py:
  - IMPLEMENT context-driven aggregations for **<TODO: your_business_metrics>**
  - ADD context attribution analysis and impact measurement
  - CREATE context quality dashboards and monitoring views
  - ENABLE context-aware ML feature engineering
  - OPTIMIZE for analytical query performance

Task Context-5:
CREATE src/jobs/context_quality_monitoring.py:
  - IMPLEMENT real-time context quality monitoring
  - ADD alerting for context SLA violations
  - CALCULATE context coverage and completeness metrics
  - TRACK context drift and distribution changes
  - GENERATE context quality reports for stakeholders

Task N:
**<TODO: Final validation and deployment tasks>**
```

### Per task pseudocode as needed added to each task
```python

# Task 1 - Bronze Layer Ingestion
# Pseudocode with CRITICAL details - customize for your data source
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

@dlt.table(
    name="bronze_**<TODO: your_source_name>**",
    comment="Raw data ingestion from **<TODO: your_data_source>**",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_id": "**<TODO: your_primary_key>** IS NOT NULL"})
def bronze_**<TODO: your_source_name>**() -> DataFrame:
    # PATTERN: Always define schema for bronze tables
    schema = StructType([
        StructField("**<TODO: your_primary_key>**", StringType(), False),
        StructField("created_at", TimestampType(), True),
        # **<TODO: Add your specific fields>**
    ])
    
    # GOTCHA: Use dlt.read() for dependencies, not spark.read
    # **<TODO: Customize data source connection for your use case>**
    return (
        spark.readStream
        .option("cloudFiles.format", "**<TODO: your_data_format>**")
        .option("cloudFiles.schemaLocation", "**<TODO: your_schema_location>**")
        .schema(schema)
        .load("**<TODO: your_data_source_path>**")
        .withColumn("_ingested_at", current_timestamp())
    )

# Task 2 - Silver Layer Transformation
@dlt.table(
    name="silver_**<TODO: your_cleaned_data_name>**",
    comment="Cleaned and transformed data for **<TODO: your_business_purpose>**"
)
@dlt.expect_all({"valid_timestamp": "created_at IS NOT NULL"})
def silver_**<TODO: your_cleaned_data_name>**() -> DataFrame:
    # PATTERN: Reference bronze tables using dlt.read()
    bronze_df = dlt.read("bronze_**<TODO: your_source_name>**")
    
    # CRITICAL: Apply business transformations
    # **<TODO: Add your specific business logic>**
    return (
        bronze_df
        .filter(col("**<TODO: your_primary_key>**").isNotNull())
        .withColumn("processed_at", current_timestamp())
        # **<TODO: Add your transformation logic>**
        .select("**<TODO: your_output_columns>**")
    )

# Context Engineering Implementation Patterns

# Task Context-1 - Multi-Source Context Ingestion
import dlt
from pyspark.sql.functions import current_timestamp, col, when, lit, hash, row_number, lag
from pyspark.sql.window import Window

@dlt.table(
    name="bronze_context_**<TODO: your_context_type>**",
    comment="Raw context data with temporal tracking and conflict detection",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "context.domain": "**<TODO: your_context_domain>**"
    }
)
@dlt.expect_all_or_drop({
    "valid_context_id": "context_id IS NOT NULL AND LENGTH(context_id) > 0",
    "valid_entity_link": "entity_id IS NOT NULL AND entity_type IS NOT NULL",
    "temporal_validity": "effective_from IS NOT NULL",
    "future_timestamp": "effective_from <= current_timestamp()"
})
@dlt.expect_all({
    "context_completeness": "context_data IS NOT NULL",
    "reasonable_confidence": "confidence_score IS NULL OR (confidence_score >= 0.0 AND confidence_score <= 1.0)"
})
def bronze_context_ingestion():
    """Pattern: Handle multiple context sources with schema evolution and deduplication"""
    schema = **<TODO: your_context_schema>**  # Use CONTEXT_SCHEMA or domain-specific schema
    
    return (
        spark.readStream
        .option("cloudFiles.format", "**<TODO: your_context_format>**")
        .option("cloudFiles.schemaEvolution", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load("**<TODO: context_source_path>**")
        .withColumn("ingested_at", current_timestamp())
        .withColumn("version", monotonically_increasing_id())
        .withColumn("context_hash", hash(col("context_data")))  # For deduplication
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )

# Task Context-2 - Context Resolution Engine with Conflict Handling
@dlt.table(
    name="silver_resolved_context",
    comment="Entity-resolved context with conflict resolution and confidence scoring",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all({
    "context_completeness": "COUNT(*) >= **<TODO: minimum_context_count>**",
    "temporal_consistency": "effective_from <= effective_to OR effective_to IS NULL",
    "confidence_threshold": "confidence_score >= **<TODO: minimum_confidence_threshold>**"
})
@dlt.expect_or_fail({
    "no_duplicate_contexts": "COUNT(*) = COUNT(DISTINCT context_id)",
    "resolution_integrity": "canonical_entity_id IS NOT NULL"
})
def context_resolution():
    """Pattern: Resolve entity context with confidence scoring and conflict resolution"""
    bronze_contexts = dlt.read("bronze_context_**<TODO: your_context_type>**")
    
    # Window for temporal conflict resolution
    temporal_window = Window.partitionBy("entity_id", "context_type").orderBy("effective_from", "confidence_score")
    
    return (
        bronze_contexts
        # Calculate confidence scores based on source reliability and data completeness
        .withColumn("source_confidence_score", 
                   when(col("source_system").isin(**<TODO: trusted_sources>**), 0.9)
                   .when(col("source_system").isin(**<TODO: known_sources>**), 0.7)
                   .otherwise(0.5))
        .withColumn("completeness_score",
                   **<TODO: your_completeness_calculation>**)  # Based on non-null context_data fields
        .withColumn("calculated_confidence",
                   (col("source_confidence_score") * 0.6 + col("completeness_score") * 0.4))
        
        # Handle temporal conflicts - keep most recent with highest confidence
        .withColumn("row_num", row_number().over(temporal_window.orderByDesc("effective_from", "calculated_confidence")))
        .filter(col("row_num") == 1)
        
        # Implement entity resolution
        .withColumn("canonical_entity_id", 
                   **<TODO: your_entity_resolution_function>**)  # Could be fuzzy matching, ML model, etc.
        
        # Update final confidence score
        .withColumn("final_confidence_score", 
                   when(col("canonical_entity_id") == col("entity_id"), col("calculated_confidence"))
                   .otherwise(col("calculated_confidence") * 0.8))  # Penalize resolved entities
        
        .select(
            col("context_id"),
            col("canonical_entity_id").alias("entity_id"),
            col("entity_type"),
            col("context_type"),
            col("context_data"),
            col("final_confidence_score").alias("confidence_score"),
            col("effective_from"),
            col("effective_to"),
            col("source_system"),
            current_timestamp().alias("resolved_at")
        )
    )

# Task Context-3 - Context Graph Enrichment with Relationship Traversal
@dlt.table(
    name="silver_enriched_context",
    comment="Graph-enriched context with derived attributes and relationship-based features"
)
@dlt.expect_all({
    "enrichment_completeness": "enriched_attributes IS NOT NULL",
    "relationship_integrity": "related_contexts IS NOT NULL OR relationship_count = 0"
})
def context_enrichment():
    """Pattern: Graph traversal for context enrichment with configurable depth"""
    resolved_context = dlt.read("silver_resolved_context")
    context_relationships = dlt.read("bronze_context_relationships")
    
    # First-degree relationship enrichment
    enriched_l1 = (
        resolved_context.alias("ctx")
        .join(
            context_relationships.alias("rel"),
            col("ctx.context_id") == col("rel.source_context_id"),
            "left"
        )
        .join(
            resolved_context.alias("related"),
            col("rel.target_context_id") == col("related.context_id"),
            "left"
        )
        .groupBy("ctx.context_id", "ctx.entity_id", "ctx.entity_type", "ctx.context_type", 
                "ctx.context_data", "ctx.confidence_score", "ctx.effective_from", "ctx.effective_to")
        .agg(
            collect_list("related.context_id").alias("related_contexts"),
            count("related.context_id").alias("relationship_count"),
            avg("related.confidence_score").alias("avg_related_confidence"),
            collect_set("related.context_type").alias("related_context_types")
        )
    )
    
    # **<TODO: Implement multi-hop traversal for your specific graph depth requirements>**
    # **<TODO: Add graph algorithms like centrality, community detection if needed>**
    
    return (
        enriched_l1
        # Calculate derived context attributes
        .withColumn("context_centrality_score",
                   when(col("relationship_count") > 0, col("relationship_count") * col("avg_related_confidence"))
                   .otherwise(0.0))
        
        # Temporal context features
        .withColumn("context_age_hours",
                   (unix_timestamp(current_timestamp()) - unix_timestamp(col("effective_from"))) / 3600)
        
        # Create enriched attributes map
        .withColumn("enriched_attributes",
                   create_map(
                       lit("relationship_count"), col("relationship_count").cast("string"),
                       lit("centrality_score"), col("context_centrality_score").cast("string"),
                       lit("age_hours"), col("context_age_hours").cast("string"),
                       **<TODO: your_additional_derived_attributes>**
                   ))
        
        .withColumn("enrichment_timestamp", current_timestamp())
        .select(
            col("context_id"),
            col("entity_id"),
            col("entity_type"),
            col("context_type"),
            col("context_data"),
            col("enriched_attributes"),
            col("confidence_score"),
            col("relationship_count"),
            col("related_contexts"),
            col("effective_from"),
            col("effective_to"),
            col("enrichment_timestamp")
        )
    )

# Task Context-4 - Context Analytics and Business Metrics
@dlt.table(
    name="gold_context_metrics",
    comment="Context quality and attribution analytics for business intelligence",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def context_analytics():
    """Pattern: Context-driven business metrics with quality tracking"""
    enriched_context = dlt.read("silver_enriched_context")
    
    return (
        enriched_context
        .withColumn("date_partition", to_date(col("effective_from")))
        .withColumn("hour_partition", hour(col("effective_from")))
        
        # Context quality dimensions
        .withColumn("is_high_quality", 
                   col("confidence_score") >= **<TODO: your_quality_threshold>**)
        .withColumn("is_well_connected",
                   col("relationship_count") >= **<TODO: your_connectivity_threshold>**)
        .withColumn("is_fresh",
                   col("context_age_hours") <= **<TODO: your_freshness_threshold>**)
        
        .groupBy("entity_type", "context_type", "date_partition", "hour_partition")
        .agg(
            count("*").alias("total_contexts"),
            countDistinct("entity_id").alias("unique_entities"),
            avg("confidence_score").alias("avg_confidence"),
            sum(when(col("is_high_quality"), 1).otherwise(0)).alias("high_quality_contexts"),
            sum(when(col("is_well_connected"), 1).otherwise(0)).alias("connected_contexts"),
            sum(when(col("is_fresh"), 1).otherwise(0)).alias("fresh_contexts"),
            avg("relationship_count").alias("avg_relationships"),
            max("confidence_score").alias("max_confidence"),
            min("confidence_score").alias("min_confidence"),
            **<TODO: your_context_specific_business_metrics>**
        )
        
        # Calculate quality scores
        .withColumn("quality_score",
                   (col("high_quality_contexts").cast("double") / col("total_contexts")) * 100)
        .withColumn("connectivity_score",
                   (col("connected_contexts").cast("double") / col("total_contexts")) * 100)
        .withColumn("freshness_score",
                   (col("fresh_contexts").cast("double") / col("total_contexts")) * 100)
    )

# Task Context-5 - Context Quality Monitoring
@dlt.table(
    name="gold_context_quality_monitoring",
    comment="Real-time context quality monitoring and SLA tracking"
)
def context_quality_monitoring():
    """Pattern: Real-time quality monitoring with alerting thresholds"""
    enriched_context = dlt.read("silver_enriched_context")
    
    # **<TODO: Implement your specific quality monitoring logic>**
    # **<TODO: Add SLA violation detection and alerting>**
    # **<TODO: Calculate context drift metrics>**
    
    return (
        enriched_context
        .withColumn("quality_check_timestamp", current_timestamp())
        .withColumn("sla_violations",
                   array(
                       when(col("confidence_score") < **<TODO: confidence_sla>**, lit("low_confidence")).otherwise(lit(None)),
                       when(col("context_age_hours") > **<TODO: freshness_sla>**, lit("stale_context")).otherwise(lit(None)),
                       **<TODO: additional_sla_checks>**
                   ).filter(lambda x: x.isNotNull()))
        .filter(size(col("sla_violations")) > 0)  # Only keep violations for alerting
    )

# **<TODO: Add additional context engineering functions for your specific domain>**
```

### Integration Points
```yaml
DATABRICKS_ASSET_BUNDLE:
  - file: databricks.yml
  - add_resource: 
      name: "**<TODO: your_pipeline_name>**"
      type: "pipelines"
      configuration: "resources/pipelines.yml"
  
PIPELINE_CONFIGURATION:
  - file: resources/pipelines.yml
  - pattern: |
      **<TODO: your_pipeline_name>**:
        name: "${var.pipeline_name}"
        target: "${var.environment}"
        libraries:
          - file:
              path: "./src/pipelines"
        clusters:
          - label: "default"
            node_type_id: "${var.node_type}"
            num_workers: "${var.num_workers}"
            # **<TODO: Add your specific cluster configuration>**
  
ENVIRONMENT_CONFIG:
  - file: environments/dev.yml
  - add_variables:
      pipeline_name: "**<TODO: your-pipeline-name>**-dev"
      node_type: "**<TODO: your_preferred_node_type>**"
      num_workers: **<TODO: your_dev_worker_count>**
      # **<TODO: Add your environment-specific variables>**
```

## Validation Loop

### Level 1: Syntax & Configuration Validation
```bash
# Run these FIRST - fix any errors before proceeding
databricks bundle validate --environment dev    # Validate Asset Bundle config
python -m py_compile src/pipelines/**/*.py      # Check Python syntax

# **<TODO: Add any project-specific validation commands>**
# Expected: No errors. If errors, READ the error and fix.
```

### Level 2: Unit Tests for Pipeline Logic
```python
# CREATE test_**<TODO: your_pipeline_name>**.py with these test cases:
import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch

def test_bronze_ingestion_schema():
    """Verify bronze table schema is correct for **<TODO: your_data_source>**"""
    # Mock dlt.read() and test schema validation
    with patch('dlt.read') as mock_read:
        mock_read.return_value = create_mock_dataframe()
        result = bronze_**<TODO: your_source_name>**()
        assert "**<TODO: your_primary_key>**" in result.columns
        assert "created_at" in result.columns
        # **<TODO: Add your specific column validations>**

def test_silver_transformation():
    """Verify silver layer transformations for **<TODO: your_business_logic>**"""
    # Test data quality expectations
    with patch('dlt.read') as mock_read:
        mock_read.return_value = create_test_bronze_data()
        result = silver_**<TODO: your_cleaned_data_name>**()
        assert result.filter(col("**<TODO: your_primary_key>**").isNull()).count() == 0
        # **<TODO: Add your specific business logic tests>**

def test_data_quality_expectations():
    """Verify DLT expectations work correctly for **<TODO: your_quality_rules>**"""
    # Test that malformed data is properly handled
    # **<TODO: Add tests for your specific data quality rules>**
    pass

# Context Engineering Test Cases

def test_context_resolution_logic():
    """Verify context resolution handles conflicts correctly"""
    # Test multiple contexts for same entity with different confidence scores
    test_contexts = create_conflicting_context_data()
    result = context_resolution_function(test_contexts)
    
    # Verify highest confidence context wins
    assert result.filter(col("confidence_score") < 0.8).count() == 0
    # Verify no duplicate canonical entity IDs
    assert result.groupBy("canonical_entity_id").count().filter(col("count") > 1).count() == 0
    # **<TODO: Add your conflict resolution validation logic>**

def test_temporal_context_validity():
    """Verify temporal context windows are handled correctly"""
    # Test overlapping effective periods
    test_data = create_temporal_context_data()
    result = temporal_context_function(test_data)
    
    # Verify temporal consistency
    invalid_temporal = result.filter(
        (col("effective_to").isNotNull()) & 
        (col("effective_from") > col("effective_to"))
    )
    assert invalid_temporal.count() == 0
    
    # Test point-in-time context retrieval
    point_in_time = datetime(2024, 1, 15, 10, 0, 0)
    active_contexts = result.filter(
        (col("effective_from") <= lit(point_in_time)) &
        ((col("effective_to").isNull()) | (col("effective_to") > lit(point_in_time)))
    )
    # **<TODO: Add your temporal logic validation>**

def test_context_graph_traversal():
    """Verify context relationship traversal works correctly"""
    # Test multi-hop context enrichment
    test_contexts = create_graph_context_data()
    test_relationships = create_context_relationships_data()
    
    result = context_graph_enrichment(test_contexts, test_relationships)
    
    # Verify relationship counts are correct
    assert result.filter(col("relationship_count") < 0).count() == 0
    # Verify centrality scores are calculated
    assert result.filter(col("context_centrality_score").isNull()).count() == 0
    # **<TODO: Add graph traversal validation for your specific algorithms>**

def test_context_quality_metrics():
    """Verify context quality calculations are accurate"""
    test_contexts = create_quality_test_data()
    result = context_quality_function(test_contexts)
    
    # Test completeness score calculation
    complete_contexts = result.filter(col("completeness_score") == 1.0)
    incomplete_contexts = result.filter(col("completeness_score") < 1.0)
    # Verify completeness logic
    
    # Test freshness score calculation
    fresh_contexts = result.filter(col("freshness_score") >= 0.8)
    stale_contexts = result.filter(col("freshness_score") < 0.5)
    # **<TODO: Add your quality metric validations>**

def test_context_sla_monitoring():
    """Verify SLA violation detection works correctly"""
    test_contexts = create_sla_violation_data()
    result = context_sla_monitoring(test_contexts)
    
    # Verify SLA violations are detected
    confidence_violations = result.filter(array_contains(col("sla_violations"), "low_confidence"))
    freshness_violations = result.filter(array_contains(col("sla_violations"), "stale_context"))
    
    assert confidence_violations.count() > 0  # Should detect low confidence
    assert freshness_violations.count() > 0   # Should detect stale context
    # **<TODO: Add your SLA monitoring validations>**

def test_context_entity_resolution():
    """Verify entity resolution algorithms work correctly"""
    test_entities = create_entity_resolution_test_data()
    result = entity_resolution_function(test_entities)
    
    # Test fuzzy matching accuracy
    correctly_resolved = result.filter(col("resolution_confidence") >= 0.9)
    # Test that duplicate entities are merged
    unique_entities = result.select("canonical_entity_id").distinct().count()
    # **<TODO: Add your entity resolution accuracy tests>**

# **<TODO: Add additional test cases for your business logic>**
```

```bash
# Run and iterate until passing:
python -m pytest tests/test_**<TODO: your_pipeline_name>**.py -v
# If failing: Read error, understand root cause, fix code, re-run
```

### Level 3: Integration Test with Asset Bundle
```bash
# Deploy to dev environment
databricks bundle deploy --environment dev

# Trigger pipeline run
databricks jobs run-now --job-id <TODO: your_pipeline_name>

# Monitor pipeline execution
databricks jobs get-run <run_id>

# Verify data quality
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM <TODO: your_bronze_table_name>",
  "warehouse_id": "<TODO: your warehouse id>"
}

# **<TODO: Add your specific data validation queries>**
# Expected: Pipeline completes successfully with expected row counts
# If error: Check Databricks job logs and DLT event logs
```

## Final validation Checklist
- [ ] Asset Bundle validates: `databricks bundle validate --environment dev`
- [ ] All tests pass: `python -m pytest tests/ -v`
- [ ] Pipeline deploys successfully: `databricks bundle deploy --environment dev`
- [ ] Manual pipeline run successful: Check Databricks UI
- [ ] Data quality expectations met: No dropped rows unexpectedly
- [ ] Performance within acceptable limits: Check pipeline metrics
- [ ] Documentation updated: README.md and pipeline comments
- [ ] **<TODO: Add your project-specific validation criteria>**

---

## Anti-Patterns to Avoid
- ❌ Don't use spark.read() directly in DLT tables - use dlt.read() for dependencies
- ❌ Don't skip data quality expectations - they're critical for pipeline reliability
- ❌ Don't hardcode paths or cluster configs - use Asset Bundle variables
- ❌ Don't use display() in DLT functions - return DataFrames
- ❌ Don't ignore DLT event logs when debugging - they contain crucial info
- ❌ Don't mix streaming and batch patterns without understanding implications
- ❌ Don't deploy directly to prod - always test in dev environment first
### Context Engineering Anti-Patterns
- ❌ Don't ignore temporal context - always track effective periods and version changes
- ❌ Don't assume context is static - implement versioning and change tracking for evolving context  
- ❌ Don't skip conflict resolution - multiple sources will have conflicting context that must be resolved
- ❌ Don't ignore context quality - implement confidence scoring, completeness metrics, and validation
- ❌ Don't flatten context relationships - preserve graph structures and relationship metadata in silver layer
- ❌ Don't batch-process real-time context - use streaming for time-sensitive context with low latency requirements
- ❌ Don't ignore context lineage - track context source, transformation history, and data provenance
- ❌ Don't over-engineer entity resolution - start with simple matching before complex ML approaches
- ❌ Don't skip context freshness validation - stale context can be worse than no context
- ❌ Don't ignore context graph performance - index relationship tables and limit traversal depth
- ❌ Don't mix context types in single tables - separate behavioral, demographic, and transactional context
- ❌ Don't assume context completeness - handle missing context gracefully with default values
- ❌ Don't ignore context privacy - implement proper PII handling and access controls for sensitive context
- ❌ Don't skip context validation - validate context against business rules and referential integrity
- ❌ Don't ignore context drift - monitor context distribution changes that may indicate data quality issues
- ❌ **<TODO: Add anti-patterns specific to your business domain>**