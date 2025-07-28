# Generate PRP (Enhanced)

## Feature specification: $ARGUMENTS

Generate a comprehensive, context-rich PRP for Databricks feature implementation that enables one-pass AI execution with iterative self-validation.

## Research & Context Gathering

### 1. Codebase Intelligence Gathering
Execute these searches in parallel to build comprehensive context:

```bash
# Asset Bundle Analysis
find . -name "databricks.yml" -o -name "*.bundle.yml" | head -5
find . -name "resources" -type d | head -3
grep -r "bundle.target" --include="*.yml" . | head -5
```

### 2. Context Engineering Strategy
- **Pattern Mining**: Extract successful patterns from existing implementations
- **Anti-Pattern Detection**: Identify common failure modes in codebase
- **Dependency Mapping**: Understand Asset Bundle resource relationships
- **Governance Compliance**: Ensure Unity Catalog alignment

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

### Context-Rich Template Population
Using `PRPs/templates/prp_base.md`, ensure these sections are comprehensive:

#### All Needed Context Section
```yaml
# MANDATORY - Primary development reference
- file: CLAUDE.md
  why: "Complete Databricks development patterns, Asset Bundle configurations, and all external documentation references"
  extract: "All sections for comprehensive context - see Consolidated Documentation References section"

- file: databricks.yml
  why: "Asset Bundle variable and target configuration template"
  extract: "Environment separation pattern and resource dependencies"

```

#### Implementation Blueprint with Databricks Specificity
```yaml
Task_1_Asset_Bundle_Setup:
  MODIFY: databricks.yml
    - ADD databricks app resource: {feature_name}_app
    - CONFIGURE targets: dev/staging/prod with Unity Catalog variables
    - SET dependencies: compute resources
    
  CREATE: resources/app.yml
    - DEFINE App configuration
    - SET specifications
    - CONFIGURE source paths and target schemas

    
Task_3_Validation_Loop_Setup:
  IMPLEMENT: Executable validation commands in order:
    1. databricks bundle validate --target dev
    2. python -m pytest tests  
    3. databricks bundle deploy --target dev
```

### Comprehensive Validation Gates

#### Multi-Level Validation Strategy
```bash
# Level 1: Configuration and Syntax Validation
databricks bundle validate --target dev

# Level 2: Asset Bundle Deployment
databricks bundle deploy --target dev --force-lock

# Level 3: Environment Cleanup (after testing)
databricks bundle destroy -t dev
```

## Output Requirements

### PRP Quality Scoring Matrix
Score each dimension 1-10, target 8+ overall for one-pass implementation success:

#### Context Completeness (Weight: 30%)
- [ ] All necessary documentation URLs included with specific why statements
- [ ] Real code examples extracted from codebase with line references  
- [ ] Anti-patterns and failure modes documented
- [ ] Existing implementation patterns identified and referenced

#### Implementation Specificity (Weight: 25%)
- [ ] Concrete Databricks patterns, not generic data engineering approaches
- [ ] Unity Catalog three-part naming conventions enforced
- [ ] Asset Bundle resource dependencies clearly defined

#### Validation Executability (Weight: 25%)
- [ ] All validation commands are Databricks CLI compatible
- [ ] Multi-level validation strategy with clear success criteria
- [ ] Error recovery and rollback procedures defined


#### Governance Compliance (Weight: 10%)
- [ ] Unity Catalog naming and governance patterns enforced
- [ ] Security and access control considerations documented

#### Performance Optimization (Weight: 10%)
- [ ] Appropriate cluster sizing and scaling configurations
- [ ] Partitioning and Z-ordering strategies specified
- [ ] Streaming trigger and batch size optimization
- [ ] Resource allocation and cost optimization considerations

### Success Criteria
- [ ] AI agent can execute PRP autonomously with 90%+ success rate
- [ ] All validation commands are Databricks Asset Bundle compatible
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

Save as: `PRPs/{feature-name}.md` with confidence score 8-10/10 for one-pass implementation success.

Remember: The goal is one-pass Databricks implementation success through comprehensive Asset Bundle and Unity Catalog context.