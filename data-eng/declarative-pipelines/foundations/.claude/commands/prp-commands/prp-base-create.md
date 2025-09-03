# Generate PRP

## Feature file: $ARGUMENTS

**🚨 MANDATORY: Feature specification is required for PRP generation**

### Feature Specification Format:
```bash
/prp-commands:prp-base-create <FEATURE_NAME> <DATA_SOURCES> <BUSINESS_REQUIREMENTS>
```

### Smart Default Behavior (Recommended):
When you run `/prp-commands:prp-base-create` without arguments:

**Default Feature**: `patient_data_medallion_pipeline` (complete healthcare LDP)
**Default Scope**: Full medallion architecture with all 3 business entities
**Default Sources**: EHR_SYSTEM, ADT_SYSTEM, LAB_SYSTEM
**Default Requirements**: HIPAA compliance, 99.5% data quality, identity resolution

**To Use Defaults**:
```bash
/prp-commands:prp-base-create --use-defaults
```

**To Override Defaults**:
```bash
/prp-commands:prp-base-create --feature="bronze_patient_ingestion" --sources="EHR_SYSTEM" --scope="ingestion_only"
```

### Examples:
```bash
# Build complete patient data pipeline (default)
/prp-commands:prp-base-create

# Build specific component
/prp-commands:prp-base-create "bronze_patient_ingestion" "EHR_SYSTEM" "Real-time patient data ingestion with schema evolution"

# Build identity resolution layer
/prp-commands:prp-base-create "patient_identity_resolution" "EHR_SYSTEM,ADT_SYSTEM,LAB_SYSTEM" "Multi-source patient matching with clinical validation"

# Build with custom requirements
/prp-commands:prp-base-create "patient_data_medallion_pipeline" "EHR_SYSTEM,ADT_SYSTEM,LAB_SYSTEM" "HIPAA compliance, 99.5% data quality, identity resolution, real-time monitoring"
```

### Automatic Feature Detection:
If no feature is specified, the system will automatically:

1. **Analyze the LDP base file** to identify the core feature:
   - **Primary Feature**: `patient_data_medallion_pipeline`
   - **Data Sources**: `EHR_SYSTEM`, `ADT_SYSTEM`, `LAB_SYSTEM`
   - **Business Entities**: `Patients`, `Claims`, `Medical_Events`
   - **Architecture**: `Bronze → Silver → Gold medallion with identity resolution`
   - **Compliance**: `HIPAA`, `HL7 FHIR`, `Data Quality 99.5%+`

2. **Generate PRP for the complete pipeline** using the base file specifications

3. **Proceed with implementation** without requiring additional clarification

### What Happens Without Feature Specification:
- ❌ System asks for clarification (current behavior)
- ❌ No specific implementation can be generated
- ❌ PRP remains generic and non-actionable

### What Happens With Smart Defaults:
- ✅ System automatically detects intended feature from LDP base file
- ✅ Generates comprehensive PRP for complete healthcare LDP pipeline
- ✅ Enables one-pass AI implementation with 8+/10 quality score
- ✅ No manual clarification required

---

Generate a comprehensive, context-rich PRP for Databricks feature implementation that enables one-pass AI execution with iterative self-validation.

## Research & Context Gathering

### 1. Codebase Intelligence Gathering
Execute these searches in parallel to build comprehensive context:

```bash
# Asset Bundle Analysis
find . -name "databricks.yml" -o -name "*.bundle.yml" | head -5
find . -name "resources" -type d | head -3
grep -r "bundle.target" --include="*.yml" . | head -5

# DLT Pipeline Pattern Discovery  
find . -path "*/pipelines/*" -name "*.py" | head -10
find . -name "*bronze*" -o -name "*silver*" -o -name "*gold*" | head -10
grep -r "dlt.read\|dlt.read_stream" --include="*.py" . | head -5

# Unity Catalog Usage Patterns
grep -r "dlt.table" --include="*.py" . | head -5
grep -r "@dlt.expect" --include="*.py" . | head -5
grep -r "CATALOG.*SCHEMA" --include="*.yml" . | head -5

# Data Quality and Governance Patterns
grep -r "table_properties" --include="*.py" . | head -5
grep -r "pipelines.pii" --include="*.py" . | head -3
```

### 2. Context Engineering Strategy
- **Pattern Mining**: Extract successful patterns from existing implementations
- **Anti-Pattern Detection**: Identify common failure modes in codebase
- **Dependency Mapping**: Understand Asset Bundle resource relationships
- **Governance Compliance**: Ensure Unity Catalog and PII handling alignment

### 3. AI Agent Context Optimization
Build context package that enables autonomous execution:

#### Critical Documentation URLs (include in PRP)
- **Primary Reference**: `CLAUDE.md` - Consolidated Documentation References section contains all Databricks platform documentation
- **Domain-Specific Only**: Include external documentation specific to your business domain (e.g., healthcare, finance, retail)

#### Real Implementation Examples (extract from codebase)
```python
# Include actual working patterns from codebase:
# - Bronze layer ingestion with schema enforcement
# - Silver layer transformation with data quality
# - Gold layer aggregation with Unity Catalog naming
# - Asset Bundle configuration snippets
```

## PRP Generation Framework

Using `PRPs/ldp_medallion_hls_base.md`, ensure these sections are comprehensive:

#### All Needed Context Section
```yaml
# MANDATORY - Primary development reference
- file: CLAUDE.md
  why: "Complete Databricks development patterns, Asset Bundle configurations, DLT pipeline examples, and all external documentation references"
  extract: "All sections for comprehensive context - see Consolidated Documentation References section"

# Domain-specific documentation only (example for healthcare)
- url: https://www.hl7.org/fhir/patient.html
  why: "FHIR Patient resource specifications for {specific_healthcare_feature}"
```

#### Implementation Blueprint with Databricks Specificity
```yaml
Task_1_Asset_Bundle_Setup:
  MODIFY: databricks.yml
    - ADD pipeline resource: {feature_name}_pipeline
    - CONFIGURE targets: dev/staging/prod with Unity Catalog variables
    - SET dependencies: existing pipelines, compute resources
    
  CREATE: resources/pipelines.yml
    - DEFINE DLT pipeline configuration
    - SET cluster specifications and scaling
    - CONFIGURE source paths and target schemas

Task_2_Bronze_Layer_Implementation:
  CREATE: src/pipelines/bronze/{source_name}_ingestion.py
    - USE schema enforcement pattern from existing bronze layer
    - IMPLEMENT dlt.table with expect_all_or_drop for critical validations
    - CONFIGURE cloud files auto loader with schema evolution
    
Task_3_Validation_Loop_Setup:
  IMPLEMENT: Executable validation commands in order:
    1. databricks bundle validate --target dev
    2. python -m py_compile src/pipelines/**/*.py  
    3. databricks bundle deploy --target dev
    4. databricks jobs run-now --job-id {pipeline_job_id}
```

### Comprehensive Validation Gates

#### Multi-Level Validation Strategy
```bash
# Level 1: Configuration and Syntax Validation
databricks bundle validate --target dev
python -m py_compile src/pipelines/**/*.py

# Level 2: Asset Bundle Deployment
databricks bundle deploy --target dev --force-lock

# Level 3: DLT Pipeline Validation and Execution
databricks bundle run --validate-only <pipeline-name>
databricks bundle run -t dev <pipeline-name>

# Level 4: Job Workflow Execution
databricks bundle run -t dev <job-name>

# Level 5: Unity Catalog Governance Validation
databricks catalogs list
databricks schemas list --catalog-name {catalog}
databricks tables list --catalog-name {catalog} --schema-name {schema} | grep {feature_tables}

# Level 6: Data Quality and Pipeline Events Monitoring
# Get pipeline ID first
PIPELINE_ID=$(databricks pipelines list-pipelines --output json | jq -r '.[] | select(.name | contains("{feature_name}")) | .pipeline_id')
# Check pipeline events for data quality metrics
databricks pipelines list-pipeline-events --pipeline-id $PIPELINE_ID --output json | jq '.[] | select(.event_type == "data_quality")'

# Level 7: Environment Cleanup (after testing)
databricks bundle destroy -t dev
```

## Output Requirements

### PRP Quality Scoring Matrix
Score each dimension 1-10, target 8+ overall for one-pass implementation success:

#### Context Completeness (Weight: 25%)
- [ ] All necessary documentation URLs included with specific why statements
- [ ] Real code examples extracted from codebase with line references  
- [ ] Anti-patterns and failure modes documented
- [ ] Existing implementation patterns identified and referenced

#### Implementation Specificity (Weight: 20%)
- [ ] Concrete Databricks patterns, not generic data engineering approaches
- [ ] Specific DLT decorators and function signatures provided
- [ ] Unity Catalog three-part naming conventions enforced
- [ ] Asset Bundle resource dependencies clearly defined

#### Validation Executability (Weight: 20%)
- [ ] All validation commands are Databricks CLI compatible
- [ ] Multi-level validation strategy with clear success criteria
- [ ] Pipeline monitoring and timeout handling included
- [ ] Error recovery and rollback procedures defined

#### Data Quality Coverage (Weight: 15%)
- [ ] Comprehensive @dlt.expect_* decorators specified
- [ ] Business logic validation rules included
- [ ] Data freshness and completeness checks defined
- [ ] Performance expectations and monitoring included

#### Governance Compliance (Weight: 10%)
- [ ] Unity Catalog naming and governance patterns enforced
- [ ] PII handling and data classification included
- [ ] Security and access control considerations documented
- [ ] Change data capture and audit trail requirements

#### Performance Optimization (Weight: 10%)
- [ ] Appropriate cluster sizing and scaling configurations
- [ ] Partitioning and Z-ordering strategies specified
- [ ] Streaming trigger and batch size optimization
- [ ] Resource allocation and cost optimization considerations

### Success Criteria
- [ ] AI agent can execute PRP autonomously with 90%+ success rate
- [ ] All validation commands are Databricks Asset Bundle compatible
- [ ] DLT pipeline follows medallion architecture with proper data quality
- [ ] Unity Catalog governance and naming conventions enforced
- [ ] Performance considerations documented with specific recommendations

## Anti-Patterns to Prevent in PRP

❌ **Vague Implementation Steps**: "Implement data quality checks"
✅ **Specific Implementation Steps**: "Add @dlt.expect_all_or_drop({'valid_customer_id': 'customer_id IS NOT NULL AND LENGTH(customer_id) >= 5'}) to bronze_customers function"

❌ **Missing Context**: "Follow DLT best practices"  
✅ **Rich Context**: "Reference src/pipelines/bronze/events.py lines 23-45 for schema enforcement pattern, specifically the StructType definition and cloudFiles.schemaLocation configuration"

❌ **Generic Validation**: "Test the pipeline"
✅ **Executable Validation**: "Run 'databricks jobs run-now --job-id {pipeline_job_id}' and verify completion with 'databricks jobs get-run {run_id}'"

## Final Quality Score: ___/10

**Success Criteria for 8+ Score:**
- AI agent can execute PRP autonomously with 90%+ success rate
- All validation commands work without modification
- Implementation follows Databricks best practices
- Data quality and governance requirements met
- Performance considerations addressed appropriately

*** CRITICAL AFTER YOU ARE DONE RESEARCHING AND EXPLORING THE CODEBASE BEFORE YOU START WRITING THE PRP ***

*** ULTRATHINK ABOUT THE DATABRICKS ASSET BUNDLES PRP AND PLAN YOUR DLT APPROACH THEN START WRITING THE PRP ***

## Output
Save as: `PRPs/{feature-name}.md` with confidence score 8-10/10 for one-pass implementation success.

Remember: The goal is one-pass Databricks implementation success through comprehensive Asset Bundle and Unity Catalog context.

## Smart Default Feature Detection

### Automatic Feature Analysis:
When no specific feature is provided, the system will automatically analyze `PRPs/ldp_medallion_hls_base.md` and generate a PRP for:

**Feature Name**: `patient_data_medallion_pipeline`
**Scope**: Complete healthcare LDP medallion architecture
**Components**: 
- Bronze layer (3 tables: patients, claims, medical_events)
- Silver layer (3 tables: cleaned and validated)
- Gold layer (3 tables: dimensional model)
- Identity resolution layer (multi-source enrichment)
- Synthetic data generation job
- Asset Bundle configuration
- HIPAA compliance and governance

**Data Sources**: EHR_SYSTEM, ADT_SYSTEM, LAB_SYSTEM
**Business Requirements**: HIPAA compliance, 99.5% data quality, real-time monitoring, identity resolution

### No Manual Input Required:
The system will automatically:
1. ✅ Detect the intended feature from the LDP base file
2. ✅ Generate comprehensive PRP for the complete pipeline
3. ✅ Include all necessary context and implementation details
4. ✅ Enable one-pass AI implementation with 8+/10 quality score
5. ✅ Eliminate the need for manual clarification or feature specification

**Result**: Fully actionable PRP that can be executed immediately without additional user input.
