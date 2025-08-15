# Healthcare LDP Medallion Architecture PRP

## Purpose
Implement a comprehensive healthcare data pipeline using Lakeflow Declarative Pipelines with context engineering and medallion architecture for health insurance patient data.

## Core Principles
- <TODO>

## Goal
**Build a mature health insurance patient data medallion pipeline using Lakeflow Declarative Pipeline that implements comprehensive data quality controls, governance, and observability for patient demographics, claims, and medical events processing.**

## Why
- **Business value**: Establish a production-ready data platform for health insurance analytics with proper governance
- **Greenfield implementation**: Build from scratch focusing on health insurance patient workflows  
- **Data quality & compliance**: Ensure accurate, complete, and HIPAA-compliant patient data processing

## What
**A complete medallion architecture (Bronze → Silver → Gold) pipeline that processes health insurance patient data with context engineering, real-time monitoring, data quality validation, and healthcare compliance controls.**

### Success Criteria

- [ ] **Exactly 3 entity types**: Patients, Claims, Medical Events (strictly enforced domain model)
- [ ] **Complete medallion pipeline**: Bronze → Silver → Gold layers with proper data quality expectations
- [ ] **99.5% data quality score** with comprehensive validation rules and HIPAA compliance
- [ ] **Context engineering implementation**: Multi-source context ingestion, resolution, and enrichment
- [ ] **Observable pipeline metrics**: Real-time monitoring, alerting, and data governance dashboards

## Databricks Lakeflow Declarative Pipelines (LDP)

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

<!-- start specific specs here related to hls -->

### Documentation & References
```yaml
- file: CLAUDE.md
  why: Foundational Databricks development patterns and external documentation references

- url: https://www.hl7.org/fhir/patient.html
  why: FHIR Patient resource specifications for healthcare data standards and validation rules
  
- url: https://www.hipaajournal.com/hipaa-compliance-checklist/
  why: HIPAA compliance requirements for patient data handling, encryption, and audit logging
```

### Healthcare LDP Context Definitions

```yaml
context_domain: patient_healthcare # Patient clinical, demographic, and care coordination context

context_sources:
  - source: EHR_SYSTEM # Electronic Health Records system
    type: streaming # Real-time patient updates
    refresh_pattern: continuous # Critical patient data updates
    latency_requirements: 30_seconds # Maximum acceptable context staleness for patient safety
  - source: ADT_SYSTEM # Admit/Discharge/Transfer system
    type: streaming # Real-time admission events
    refresh_pattern: continuous 
    latency_requirements: 15_seconds
  - source: LAB_SYSTEM # Laboratory results system
    type: batch # Lab results updates
    refresh_pattern: hourly # Lab results processed hourly
    latency_requirements: 1_hour
    
context_resolution:
  - entity_type: patients # Primary entities requiring context
    resolution_keys: patient_id, mrn, ssn_hash, first_name_last_name_dob # Keys used for patient matching
    conflict_strategy: highest_confidence # Medical data requires highest confidence resolution
    temporal_strategy: point_in_time # Clinical context must be temporally accurate
    
context_quality_requirements:
  - completeness_threshold: 98% # Minimum patient context coverage for clinical safety
  - freshness_sla: 5_minutes # Maximum context age for active patients
  - consistency_rules: clinical_validation_rules # Cross-system clinical data validation
  - confidence_thresholds: 0.95 # Minimum confidence scores for patient identification

context_graph_structure:
  - relationship_types: hierarchical_care_team, patient_episodes, clinical_pathways # Healthcare relationships
  - traversal_depth: 3 # Maximum relationship hops for care coordination
  - graph_algorithms: care_pathway_analysis, clinical_decision_support # Healthcare-specific algorithms
```

### Known Gotchas of our codebase & Databricks Quirks
```python
# CRITICAL: Delta Live Tables requires specific decorators and patterns
# Example: @dlt.table() functions must return DataFrames, not display()
# Example: Pipeline dependencies must be explicit using dlt.read() or dlt.read_stream()

# HEALTHCARE DATA SPECIFIC GOTCHAS:
# CRITICAL: HIPAA compliance requires encryption at rest and in transit - ensure Delta tables use encryption
# CRITICAL: Patient data requires strict PII handling - never log actual patient identifiers
# CRITICAL: Use @dlt.quarantine() instead of @dlt.expect_all_or_drop() for patient data to maintain audit trail
# CRITICAL: Healthcare data has strict temporal requirements - always include effective dates and version tracking
# CRITICAL: Patient matching requires deterministic hashing for SSN/DOB - use consistent salt across pipeline
# CRITICAL: Clinical data validation must follow HL7 FHIR standards for interoperability
# CRITICAL: Medical Record Numbers (MRN) may not be unique across healthcare systems - implement proper entity resolution
# CRITICAL: Lab results and vitals have specific value ranges - implement clinical validation rules
# CRITICAL: Patient consent and data retention policies must be enforced at the data layer
# CRITICAL: Delta Live Tables streaming with patient data requires careful ordering for clinical accuracy
# CRITICAL: Healthcare audit logs are required for compliance - enable Delta change data feed on all patient tables
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

# PATIENT/HEALTHCARE DOMAIN-SPECIFIC CONTEXT SCHEMAS:

# Patient Context Schema - Core patient demographic and clinical context
PATIENT_CONTEXT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),        # Primary patient identifier
    StructField("mrn", StringType(), True),                # Medical Record Number
    StructField("ssn_hash", StringType(), True),           # Hashed SSN for privacy
    StructField("demographics", MapType(StringType(), StringType()), True), # Age, gender, race, etc.
    StructField("insurance_coverage", ArrayType(StringType()), True), # Insurance plans
    StructField("primary_care_provider", StringType(), True), # PCP identifier
    StructField("care_team_members", ArrayType(StringType()), True), # Care team IDs
    StructField("chronic_conditions", ArrayType(StringType()), True), # ICD-10 codes
    StructField("allergies", ArrayType(StringType()), True), # Known allergies
    StructField("emergency_contacts", ArrayType(MapType(StringType(), StringType())), True), # Emergency contacts
    StructField("risk_scores", MapType(StringType(), DoubleType()), True), # Clinical risk scores
    # Include base context fields from CONTEXT_SCHEMA
])

# Clinical Context Schema - Clinical events and care episodes
CLINICAL_CONTEXT_SCHEMA = StructType([
    StructField("episode_id", StringType(), False),        # Care episode identifier
    StructField("patient_id", StringType(), False),        # Patient reference
    StructField("encounter_type", StringType(), False),    # inpatient/outpatient/emergency
    StructField("admission_timestamp", TimestampType(), True), # Admission time
    StructField("discharge_timestamp", TimestampType(), True), # Discharge time
    StructField("diagnosis_codes", ArrayType(StringType()), True), # Primary/secondary ICD-10
    StructField("procedure_codes", ArrayType(StringType()), True), # CPT procedure codes
    StructField("care_pathway", StringType(), True),       # Clinical pathway/protocol
    StructField("acuity_level", IntegerType(), True),      # Clinical acuity (1-5)
    StructField("length_of_stay", IntegerType(), True),    # LOS in hours
    StructField("readmission_risk", DoubleType(), True),   # 30-day readmission risk
    # Include base context fields from CONTEXT_SCHEMA
])

# Treatment Context Schema - Medications, procedures, and interventions
TREATMENT_CONTEXT_SCHEMA = StructType([
    StructField("treatment_id", StringType(), False),      # Treatment identifier
    StructField("patient_id", StringType(), False),        # Patient reference
    StructField("episode_id", StringType(), True),         # Episode reference
    StructField("treatment_type", StringType(), False),    # medication/procedure/therapy
    StructField("medication_name", StringType(), True),    # Drug name
    StructField("dosage", StringType(), True),             # Medication dosage
    StructField("route", StringType(), True),              # Administration route
    StructField("frequency", StringType(), True),          # Dosing frequency
    StructField("start_date", TimestampType(), True),      # Treatment start
    StructField("end_date", TimestampType(), True),        # Treatment end
    StructField("prescribing_provider", StringType(), True), # Provider ID
    StructField("contraindications", ArrayType(StringType()), True), # Drug interactions
    StructField("side_effects", ArrayType(StringType()), True), # Observed side effects
    # Include base context fields from CONTEXT_SCHEMA
])
```

### list of tasks to be completed to fullfill the PRP in the order they should be completed

```yaml
Task 1:
MODIFY databricks.yml:
  - ADD new pipeline resource for patient_data_medallion_pipeline
  - CONFIGURE target environments (dev, staging, prod) with HIPAA compliance settings
  - SET appropriate compute settings for healthcare_workload with encryption at rest

CREATE src/pipelines/bronze/patient_ehr_ingestion.py:
  - IMPLEMENT EHR patient data ingestion with FHIR validation
  - USE @dlt.streaming_table() decorators for real-time patient updates
  - IMPLEMENT data quality expectations for patient_data_validation_rules with quarantine
  - ENABLE Delta change data feed for audit compliance

CREATE src/pipelines/bronze/patient_adt_ingestion.py:
  - IMPLEMENT ADT (Admit/Discharge/Transfer) event streaming
  - USE @dlt.streaming_table() for real-time admission events
  - IMPLEMENT clinical validation rules for encounter data

CREATE src/pipelines/bronze/patient_lab_ingestion.py:
  - IMPLEMENT laboratory results batch ingestion
  - USE @dlt.table() for hourly lab result processing
  - IMPLEMENT clinical range validation for lab values

CREATE src/pipelines/silver/patient_data_transformation.py:
  - IMPLEMENT patient data cleansing and clinical standardization
  - ADD HIPAA de-identification and data quality expectations
  - USE proper dlt.read() dependencies for bronze layer integration
  - IMPLEMENT HL7 FHIR compliance transformations

CREATE src/pipelines/silver/patient_entity_resolution.py:
  - IMPLEMENT patient matching across EHR, ADT, and Lab systems
  - ADD deterministic and probabilistic matching algorithms
  - ENSURE proper handling of duplicate patient records

CREATE src/pipelines/gold/patient_clinical_metrics.py:
  - IMPLEMENT clinical quality metrics and patient outcome analytics
  - ADD aggregations for readmission rates, length of stay, clinical indicators
  - ENSURE proper temporal handling for clinical reporting

CREATE src/pipelines/gold/patient_quality_dashboard.py:
  - IMPLEMENT data quality monitoring for patient pipeline observability
  - ADD real-time quality metrics and compliance dashboards
  - CREATE alerting for data quality SLA violations

Context Engineering Tasks:

Task Context-1:
CREATE src/pipelines/bronze/patient_context_ingestion.py:
  - IMPLEMENT multi-source patient context ingestion from EHR, ADT, and Lab systems
  - ADD patient context deduplication and clinical conflict detection logic
  - USE @dlt.quarantine for context quality validation (maintain audit trail for HIPAA)
  - HANDLE patient context versioning and clinical temporal windows
  - IMPLEMENT schema evolution for changing healthcare data structures

Task Context-2:  
CREATE src/pipelines/silver/patient_context_resolution.py:
  - IMPLEMENT patient entity resolution with clinical context awareness
  - ADD clinical conflict resolution logic using highest_confidence strategy
  - CALCULATE patient context confidence scores using clinical validation algorithms
  - MAINTAIN patient context lineage and clinical provenance tracking
  - HANDLE temporal clinical context alignment and care episode synchronization

Task Context-3:
CREATE src/pipelines/silver/patient_context_enrichment.py:
  - IMPLEMENT care team graph traversal for hierarchical_care_team relationships
  - ADD temporal clinical context joining with episode-based sliding windows
  - CALCULATE derived clinical attributes and care coordination metrics
  - ENSURE patient context freshness validation against 5-minute SLA requirements
  - OPTIMIZE care pathway queries for clinical decision support performance

Task Context-4:
CREATE src/pipelines/gold/patient_context_analytics.py:
  - IMPLEMENT context-driven clinical aggregations for patient outcome metrics
  - ADD clinical context attribution analysis and care quality impact measurement
  - CREATE patient context quality dashboards and HIPAA compliance monitoring views
  - ENABLE clinical context-aware ML feature engineering for predictive models
  - OPTIMIZE for clinical analytical query performance and regulatory reporting

Task Context-5:
CREATE src/jobs/patient_context_quality_monitoring.py:
  - IMPLEMENT real-time patient context quality monitoring with clinical SLA tracking
  - ADD alerting for patient context SLA violations and clinical data quality issues
  - CALCULATE patient context coverage and clinical completeness metrics
  - TRACK patient context drift and clinical distribution changes for data governance
  - GENERATE patient context quality reports for healthcare compliance stakeholders

Task Final:
VALIDATION and DEPLOYMENT for patient_data_medallion_pipeline:
  - VALIDATE HIPAA compliance across all pipeline components
  - TEST clinical data quality rules and patient safety validations
  - DEPLOY to healthcare-compliant environments with proper security controls
  - VERIFY observable pipeline functionality and clinical data governance
```

### Per task pseudocode as needed added to each task
```python

# Task 1 - Bronze Layer Patient EHR Ingestion
# Pseudocode with CRITICAL details for patient data ingestion
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import current_timestamp, hash, sha2

@dlt.table(
    name="bronze_patient_ehr",
    comment="Raw patient EHR data ingestion with HIPAA compliance and audit logging",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        # "delta.encryption.enabled": "true"     # HIPAA encryption requirement # TODO: Claud.md review
    }
)
@dlt.quarantine({"valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) > 0"})
@dlt.expect_all({"hipaa_compliant": "ssn IS NULL OR LENGTH(ssn) = 0"})  # Ensure no raw SSN
def bronze_patient_ehr() -> DataFrame:
    # PATTERN: Always define schema for patient tables with HIPAA considerations
    schema = StructType([
        StructField("patient_id", StringType(), False),        # Primary patient identifier
        StructField("mrn", StringType(), True),                # Medical Record Number
        StructField("first_name", StringType(), True),         # Patient first name
        StructField("last_name", StringType(), True),          # Patient last name
        StructField("date_of_birth", StringType(), True),      # DOB as string for privacy
        StructField("gender", StringType(), True),             # Patient gender
        StructField("address_line1", StringType(), True),      # Patient address
        StructField("city", StringType(), True),               # Patient city
        StructField("state", StringType(), True),              # Patient state
        StructField("zip_code", StringType(), True),           # Patient ZIP
        StructField("phone", StringType(), True),              # Patient phone
        StructField("email", StringType(), True),              # Patient email
        StructField("emergency_contact_name", StringType(), True), # Emergency contact
        StructField("emergency_contact_phone", StringType(), True), # Emergency phone
        StructField("primary_care_provider", StringType(), True),   # PCP ID
        StructField("insurance_id", StringType(), True),       # Insurance identifier
        StructField("effective_date", TimestampType(), False), # When record becomes effective
        StructField("source_system", StringType(), False),     # EHR system identifier
        StructField("created_at", TimestampType(), True)       # Original creation timestamp
    ])
    
    # GOTCHA: Use streaming for real-time patient updates for clinical safety
    return (
        spark.readStream
        .option("cloudFiles.format", "json")  # Common EHR export format
        .option("cloudFiles.schemaLocation", "/mnt/healthcare/schemas/patient_ehr")
        .option("cloudFiles.schemaEvolution", "true")  # Handle EHR schema changes
        .schema(schema)
        .load("/mnt/healthcare/raw/ehr/patients")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_version", lit("v1.0"))
        # CRITICAL: Hash PII for privacy compliance
        .withColumn("ssn_hash", 
                   when(col("ssn").isNotNull(), sha2(concat(col("ssn"), lit("PATIENT_SALT")), 256))
                   .otherwise(lit(None)))
        .drop("ssn")  # Remove raw SSN immediately after hashing
    )

# Task 2 - Silver Layer Patient Data Transformation
@dlt.table(
    name="silver_patient_master",
    comment="Cleaned and standardized patient data with clinical validation and HIPAA compliance",
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true",
        "pipelines.patient.compliance": "HIPAA"
    }
)
@dlt.expect_all({
    "valid_patient_timestamp": "effective_date IS NOT NULL",
    "valid_demographics": "patient_id IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL",
    "clinical_safety": "age_years >= 0 AND age_years <= 150"
})
@dlt.quarantine({"potential_test_patient": "UPPER(last_name) NOT LIKE '%TEST%' AND UPPER(first_name) NOT LIKE '%TEST%'"})
def silver_patient_master() -> DataFrame:
    # PATTERN: Reference bronze tables using dlt.read()
    bronze_ehr = dlt.read("bronze_patient_ehr")
    
    # CRITICAL: Apply clinical data transformations and HIPAA de-identification
    return (
        bronze_ehr
        .filter(col("patient_id").isNotNull())
        .withColumn("processed_at", current_timestamp())
        
        # Clinical data standardization
        .withColumn("age_years", 
                   floor(datediff(current_date(), to_date(col("date_of_birth"), "yyyy-MM-dd")) / 365.25))
        .withColumn("gender_standardized", 
                   when(upper(col("gender")).isin("M", "MALE"), "M")
                   .when(upper(col("gender")).isin("F", "FEMALE"), "F")
                   .otherwise("U"))  # Unknown
        
        # HIPAA de-identification - remove direct identifiers for age 89+
        .withColumn("age_deidentified",
                   when(col("age_years") >= 89, 90)  # HIPAA safe harbor
                   .otherwise(col("age_years")))
        .withColumn("zip_deidentified",
                   when(col("age_years") >= 89, regexp_replace(col("zip_code"), "\\d{2}$", "00"))
                   .otherwise(col("zip_code")))
        
        # Clinical data quality validations
        .withColumn("data_quality_score",
                   (when(col("first_name").isNotNull(), 0.2).otherwise(0) +
                    when(col("last_name").isNotNull(), 0.2).otherwise(0) +
                    when(col("date_of_birth").isNotNull(), 0.2).otherwise(0) +
                    when(col("mrn").isNotNull(), 0.2).otherwise(0) +
                    when(col("primary_care_provider").isNotNull(), 0.2).otherwise(0)))
        
        # Clinical context enrichment
        .withColumn("patient_risk_category",
                   when(col("age_deidentified") >= 65, "HIGH_RISK_AGE")
                   .when(col("emergency_contact_name").isNull(), "HIGH_RISK_SOCIAL")
                   .otherwise("STANDARD_RISK"))
        
        .select(
            col("patient_id"),
            col("mrn"),
            col("first_name"),
            col("last_name"), 
            col("age_deidentified").alias("age_years"),
            col("gender_standardized").alias("gender"),
            col("zip_deidentified").alias("zip_code"),
            col("phone"),
            col("email"),
            col("emergency_contact_name"),
            col("emergency_contact_phone"),
            col("primary_care_provider"),
            col("insurance_id"),
            col("ssn_hash"),
            col("patient_risk_category"),
            col("data_quality_score"),
            col("effective_date"),
            col("source_system"),
            col("processed_at")
        )
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
```

## Domain Model: Health Insurance Patient Analytics

### Architecture Overview
- **Medallion Architecture**: Bronze → Silver → Gold data transformation pipeline
- **Delta Live Tables (DLT)**: Declarative pipelines with data quality expectations  
- **Context Engineering**: Multi-source context ingestion, resolution, and enrichment
- **HIPAA Compliance**: Healthcare data governance and audit trails

### Data Sources
- **Health Insurance CSV Files**: Patient demographics, claims, medical events (Databricks Volumes)
- **Synthetic Data Generation**: On-demand realistic test data matching production schema
- **Auto Loader Ingestion**: Schema evolution with hourly batch processing

#### Entity Relationship Model (MANDATORY - STRICTLY ENFORCE)

**🚨 CRITICAL: ALL synthetic data generation and medallion architecture implementation MUST strictly adhere to this exact three-entity model. No additional entities or deviations allowed.**

```
Health Insurance Domain: Patient Analytics (EXACTLY 3 ENTITIES)
├── Patients (dim_patients) - PRIMARY ENTITY
│   ├── patient_id (PK) - String, unique identifier
│   ├── demographics (age, sex, region) - Core patient attributes
│   │   ├── age - Integer, 18-85 range (normal distribution μ=45, σ=15)
│   │   ├── sex - String, MALE/FEMALE
│   │   └── region - String, NORTHEAST/NORTHWEST/SOUTHEAST/SOUTHWEST
│   ├── health_metrics (bmi, smoker, children) - Health and lifestyle data
│   │   ├── bmi - Double, 16-50 range (normal distribution μ=28, σ=6)
│   │   ├── smoker - Boolean, age-correlated smoking probability
│   │   └── children - Integer, number of dependents (Poisson λ=1.2)
│   ├── financial_data (charges) - Insurance cost information
│   │   └── charges - Double, calculated premium based on risk factors
│   ├── insurance_details (plan_type, coverage_start_date) - Insurance information
│   ├── temporal_data (timestamp) - Record creation timestamp
│   └── SCD Type 2 for historical tracking - Track changes over time
│
├── Claims (fact_claims) - TRANSACTIONAL ENTITY
│   ├── claim_id (PK) - String, unique claim identifier
│   ├── patient_id (FK) - String, references Patients.patient_id
│   ├── claim_amount, claim_date - Financial and temporal data
│   ├── diagnosis_code, procedure_code - Medical coding (ICD-10, CPT)
│   └── claim_status - Processing status (submitted, approved, denied, paid)
│
└── Medical_History (fact_medical_events) - EVENT ENTITY
    ├── event_id (PK) - String, unique event identifier
    ├── patient_id (FK) - String, references Patients.patient_id
    ├── event_date, event_type - Temporal and categorical data
    └── medical_provider - Healthcare provider information
```

#### Mandatory Entity Implementation Requirements

**🚨 SYNTHETIC DATA GENERATION REQUIREMENTS:**
1. **Generate EXACTLY 3 CSV files**: patients.csv, claims.csv, medical_events.csv
2. **Maintain referential integrity**: All claims.patient_id and medical_events.patient_id MUST reference valid patients.patient_id
3. **Realistic ratios**: Each patient should have 2-5 claims and 3-8 medical events on average
4. **No additional entities**: Do not create provider tables, diagnosis tables, or other entities

**🚨 MEDALLION ARCHITECTURE REQUIREMENTS:**
1. **Bronze Layer**: Exactly 3 tables (bronze_patients, bronze_claims, bronze_medical_events)
2. **Silver Layer**: Exactly 3 tables (silver_patients, silver_claims, silver_medical_events) with data quality and standardization
3. **Gold Layer**: Dimensional model with dim_patients and 2 fact tables (fact_claims, fact_medical_events)
4. **Foreign Key Validation**: Silver and Gold layers MUST validate and maintain referential integrity
5. **No Schema Drift**: Additional columns or entities require explicit approval and domain model updates

### Medallion Architecture Implementation

#### Bronze Layer (Raw Data Landing)
- **Purpose**: Immutable raw data ingestion with minimal transformation
- **Data Quality**: Schema enforcement, basic validation, rescued data handling
- **Storage**: Delta tables with partition by ingestion date
- **Governance**: PII detection, data classification, audit logging

#### Silver Layer (Cleaned & Enriched)
- **Purpose**: Cleaned, deduplicated, and business-rule validated data
- **Data Quality**: Comprehensive validation, outlier detection, referential integrity
- **Transformations**: Data type standardization, business logic application
- **Governance**: Data lineage tracking, quality metrics collection

#### Gold Layer (Analytics-Ready)
- **Purpose**: Aggregated, dimensional model for analytics and ML
- **Design Pattern**: Star schema with fact and dimension tables
- **Performance**: Pre-aggregated metrics, materialized views
- **Governance**: Change data capture enabled, access controls

### Project Structure
```
healthcare-ldp-medallion/
├── src/
│   ├── pipelines/
│   │   ├── bronze/            # Raw data ingestion (3 tables)
│   │   ├── silver/            # Data cleaning & validation (3 tables)
│   │   ├── gold/              # Dimensional model (3 tables)
│   │   └── shared/            # Common schemas and utilities
│   ├── jobs/
│   │   ├── data_generation/   # Synthetic patient data generation
│   │   └── monitoring/        # Context quality monitoring
│   └── tests/                 # Unit and integration tests
└── PRPs/                      # Planning and documentation
```

## Implementation Guidelines

### Schema Standards
- **Naming Convention**: `{layer}_{entity}` (e.g., bronze_patients, silver_claims, gold_patient_metrics)
- **Data Types**: String for IDs, Integer for counts, Double for amounts, Boolean for flags
- **Primary Keys**: patient_id, claim_id, event_id for each respective entity
- **Partitioning**: By ingestion date (bronze), by business date (silver/gold)

### Mandatory Schema Implementation (DOMAIN MODEL ENFORCEMENT)

**🚨 CRITICAL: Implementation MUST create EXACTLY these tables with EXACTLY these schemas - no additions, no omissions.**

#### Bronze Layer Tables (RAW DATA - EXACTLY 3 TABLES)

**1. bronze_patients** (Raw patient demographic and insurance data)
```python
PATIENT_SCHEMA = StructType([
    # Primary Key
    StructField("patient_id", StringType(), False),      # PK - MANDATORY
    
    # Demographics
    StructField("first_name", StringType(), True),       # Demographics
    StructField("last_name", StringType(), True),        # Demographics  
    StructField("age", IntegerType(), True),             # Demographics (18-85)
    StructField("sex", StringType(), True),              # Demographics (MALE/FEMALE)
    StructField("region", StringType(), True),           # Location (NORTHEAST/NORTHWEST/SOUTHEAST/SOUTHWEST)
    
    # Health Metrics
    StructField("bmi", DoubleType(), True),              # Health metric (16-50)
    StructField("smoker", BooleanType(), True),          # Lifestyle factor
    StructField("children", IntegerType(), True),        # Number of dependents
    
    # Financial Data
    StructField("charges", DoubleType(), True),          # Calculated insurance premium
    
    # Insurance Details
    StructField("insurance_plan", StringType(), True),   # Insurance details
    StructField("coverage_start_date", StringType(), True), # Insurance details
    
    # Temporal Data
    StructField("timestamp", StringType(), True),        # Record creation timestamp
    
    # Pipeline Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True)
])
```

**2. bronze_claims** (Raw insurance claims data)
```python
CLAIMS_SCHEMA = StructType([
    StructField("claim_id", StringType(), False),        # PK - MANDATORY
    StructField("patient_id", StringType(), False),     # FK to patients - MANDATORY
    StructField("claim_amount", DoubleType(), True),    # Financial data
    StructField("claim_date", StringType(), True),      # Temporal data
    StructField("diagnosis_code", StringType(), True),  # ICD-10 code
    StructField("procedure_code", StringType(), True),  # CPT code
    StructField("claim_status", StringType(), True),    # Status (submitted/approved/denied/paid)
    # Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True)
])
```

**3. bronze_medical_events** (Raw medical history/events data)
```python
MEDICAL_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),       # PK - MANDATORY
    StructField("patient_id", StringType(), False),    # FK to patients - MANDATORY
    StructField("event_date", StringType(), True),     # Temporal data
    StructField("event_type", StringType(), True),     # Event category
    StructField("medical_provider", StringType(), True), # Provider info
    # Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True)
])
```

#### Silver Layer Requirements (DATA QUALITY - EXACTLY 3 TABLES)
- **silver_patients**: Cleaned patient data with HIPAA compliance and data quality validation
- **silver_claims**: Validated claims with referential integrity checks to silver_patients
- **silver_medical_events**: Cleaned medical events with referential integrity checks to silver_patients

#### Gold Layer Requirements (DIMENSIONAL MODEL - EXACTLY 3 TABLES)
- **dim_patients**: SCD Type 2 patient dimension with complete patient 360 view
- **fact_claims**: Claims fact table with pre-aggregated metrics and foreign key to dim_patients
- **fact_medical_events**: Medical events fact table with foreign key to dim_patients

### Key Implementation Requirements
1. **Exactly 3 entities**: Patients, Claims, Medical_Events (no additional entities)
2. **Referential integrity**: All claims/medical_events must reference valid patient_id
3. **HIPAA compliance**: PII hashing, audit trails, data quality quarantine 
5. **Context engineering**: Multi-source ingestion, resolution, enrichment patterns

### Data Quality Patterns
```python
# Health insurance data validation example
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_age": "age IS NOT NULL AND age BETWEEN 18 AND 85", 
    "valid_sex": "sex IS NOT NULL AND sex IN ('MALE', 'FEMALE')",
    "valid_bmi": "bmi IS NOT NULL AND bmi BETWEEN 16 AND 50",
    "valid_charges": "charges IS NOT NULL AND charges > 0"
})
def silver_patients():
    return dlt.read("bronze_patients")
```

## Development Phases

1. **Asset Bundle Setup**: Configure `databricks.yml` with dev/prod environments
2. **Synthetic Data Generation**: Create realistic patient/claims/medical_events CSV files  
3. **Bronze Layer**: Auto Loader ingestion with schema enforcement
4. **Silver Layer**: Data cleaning, validation, and context resolution
5. **Gold Layer**: Dimensional modeling with fact/dimension tables
6. **Context Engineering**: Multi-source context processing and quality monitoring

## Anti-Patterns to Avoid

- ❌ Don't use spark.read() directly in DLT tables - use dlt.read() for dependencies
- ❌ Don't skip data quality expectations - they're critical for pipeline reliability
- ❌ Don't use display() in DLT functions - return DataFrames
- ❌ Don't ignore DLT event logs when debugging - they contain crucial info
- ❌ Don't mix streaming and batch patterns without understanding implications
- ❌ Don't deploy directly to prod - always test in dev environment first
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
- ❌ Don't store raw SSN or other direct identifiers - always hash PII immediately upon ingestion
- ❌ Don't use @dlt.expect_all_or_drop() for patient data - use @dlt.quarantine() to maintain audit trails
- ❌ Don't assume MRNs are unique across healthcare systems - implement proper patient entity resolution
- ❌ Don't ignore clinical data ranges - implement HL7 FHIR validation for lab values and vitals
- ❌ Don't skip change data feed enablement - HIPAA requires audit trails for all patient data modifications
- ❌ Don't hardcode clinical thresholds - use configurable parameters for age limits and clinical ranges
- ❌ Don't process patient data without encryption - ensure Delta tables have encryption enabled
- ❌ Don't ignore temporal clinical context - patient data must maintain point-in-time clinical accuracy
- ❌ Don't deploy patient pipelines without HIPAA compliance testing and validation
