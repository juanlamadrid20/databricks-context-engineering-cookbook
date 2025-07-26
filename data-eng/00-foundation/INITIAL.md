# <TODO: Insert Project Name> Data Engineering Project

## PROJECT OVERVIEW

This project implements a modern data engineering solution for <TODO: describe your business domain and use case> featuring:

- **Synthetic Data Generation**: Automated generation of realistic test data for <TODO: specify your data sources and entity types>
- **Medallion Architecture**: Bronze → Silver → Gold data transformation pipeline
- **Delta Live Tables (DLT)**: Declarative pipelines for data processing and quality management
- **Databricks Asset Bundle**: Infrastructure-as-code deployment and management

## DEPLOYMENT APPROACH

**This project MUST use Databricks Asset Bundles for all deployment and management activities.**

### Why Asset Bundles?
- **Infrastructure-as-Code**: Version-controlled deployment configurations
- **Environment Management**: Seamless promotion between dev/staging/prod
- **Integrated Workflow**: Native support for DLT pipelines, jobs, and compute resources
- **Declarative Configuration**: YAML-based configuration for reproducible deployments
- **Best Practice**: Recommended approach for modern Databricks project management

## ARCHITECTURE

### Source Schema & Tables
**Define your source schema and data model here:**
<TODO: Document your source systems, data formats, and entity relationships>
<TODO: Include links to schema documentation, ERD diagrams, or data dictionaries>
<TODO: Specify data sources (APIs, databases, files, etc.) and their refresh patterns>

### Medallion Architecture Implementation
- **Bronze Layer**: Raw data ingestion and landing zone for <TODO: specify your data sources>
- **Silver Layer**: Cleaned, validated, and enriched data with <TODO: define your data quality rules>
- **Gold Layer**: Business-ready dimensional data warehouse with <TODO: specify your dimensional model approach>

### Data Warehouse Design
<TODO: Describe your dimensional model design (star schema, snowflake, etc.)>
<TODO: Define your fact tables, dimension tables, and key business metrics>
<TODO: Specify your slowly changing dimension (SCD) strategies>

## PROJECT STRUCTURE

```
<TODO: Update project name>/
├── PRPs/                        # Problem Requirements & Proposals
│   └── templates/               # PRP templates
└── src/                         # Source code (to be created)
    ├── data_generation/         # Synthetic data generation jobs
    ├── pipelines/               # DLT pipeline definitions
    └── tests/                   # Unit and integration tests
```

## DEVELOPMENT SETUP

### Prerequisites
- **Databricks CLI configured** (required for Asset Bundle deployment)
- **Python 3.12+** with databricks-sdk
- **Access to Databricks workspace** with DLT capabilities
- **Asset Bundle permissions** for target workspace and environments
- <TODO: Add any specific tools, libraries, or access requirements for your project>

### Getting Started
1. **Initialize Asset Bundle structure** using `databricks bundle init`
2. **Review the example implementations** in `examples/` (especially `databricks.yml`)
3. **Configure your bundle** for dev/staging/prod environments
4. **Use the PRP templates** in `PRPs/templates/` for planning
5. **Deploy using Asset Bundles**: `databricks bundle deploy`
6. **Follow the architecture patterns** demonstrated in examples

### Asset Bundle Workflow
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

## EXAMPLES & REFERENCES

### Code Examples
- **Data Generation**: `examples/job_data_gen.py` - Synthetic data generation patterns
- **Pipeline Processing**: `examples/pipeline_main.py` - Medallion architecture implementation
- **Asset Bundle Configuration**: `examples/databricks.yml` - **PRIMARY REFERENCE for Asset Bundle setup**

> **Note**: These examples serve as templates for best practices. Adapt them to your specific <TODO: business domain and data requirements>.

### Asset Bundle Configuration
The `examples/databricks.yml` file provides the template for:
- **Job definitions** for synthetic data generation
- **DLT pipeline configurations** for medallion architecture
- **Environment-specific settings** (dev/staging/prod)
- **Resource permissions** and compute configurations
- **Deployment targets** and workspace settings

### External Code References
- Use **dbrx-snippets MCP server** for additional code examples and patterns
- Access via MCP for up-to-date Databricks code snippets

## DOCUMENTATION

### Primary Documentation Sources
- **Context7 MCP Server**: For best practices and updated documentation
- **Databricks DLT Documentation**:
  - [DLT Development Guide](https://docs.databricks.com/aws/en/dlt/develop)
  - [Python DLT Development](https://docs.databricks.com/aws/en/dlt/python-dev)

### Additional Resources
- **<TODO: Add your primary schema/business documentation sources>**
- **<TODO: Include links to your data governance policies and standards>**
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Medallion Architecture Best Practices](https://www.databricks.com/glossary/medallion-architecture)

## DEVELOPMENT GUIDELINES

### Schema Implementation
- **<TODO: Define your schema naming conventions and standards>**
- **<TODO: Specify your data type mapping and constraint rules>**
- **<TODO: Document your primary key and foreign key relationship patterns>**
- **<TODO: Define your table partitioning and optimization strategies>**

### Source Schema Elements
<TODO: Document your specific schema elements>
- **<TODO: Primary fact tables>**: Complete table structure with measures and foreign keys
- **<TODO: Key dimension tables>**: Including surrogate keys, natural keys, and SCD patterns
- **Identity Column Patterns**: Sequential surrogate key generation
- **Missing Member Entries**: Handling unknown dimension references
- **Data Types Mapping**: Databricks-specific data type implementations
- **Foreign Key Relationships**: Proper fact-to-dimension linkages
- **Metadata Field Patterns**: ETL support columns and structures

### Best Practices
1. **Asset Bundle First**: Always use Asset Bundles for deployment - never deploy manually
2. **Environment Isolation**: Use separate Asset Bundle targets for dev/staging/prod
3. **Version Control**: All databricks.yml configurations must be version controlled
4. **Configuration via Variables**: Use Asset Bundle variables and `spark.conf.get()` pattern
5. **Data Quality**: Implement comprehensive data quality checks in DLT pipelines using `@dlt.expect_*` decorators
6. **Path Handling**: Include robust Databricks path handling in all pipeline files
7. **Testing**: Include unit tests for data generation logic
8. **Documentation**: Document data lineage and transformation logic

### Proven Configuration Patterns
#### Asset Bundle Variable Management
```yaml
variables:
  catalog:
    description: "Unity Catalog name"
    default: "<TODO: your_default_catalog>"
  schema:
    description: "Schema name for your data"
    default: "<TODO: your_default_schema>"
  max_files_per_trigger:
    description: "Performance tuning parameter"
    default: 100

targets:
  dev:
    variables:
      catalog: "<TODO: your_dev_catalog>"
      schema: "<TODO: your_dev_schema>"
      max_files_per_trigger: 100
  prod:
    variables:
      catalog: "<TODO: your_prod_catalog>"
      schema: "<TODO: your_prod_schema>"
      max_files_per_trigger: 1000
```

#### Pipeline Configuration Pattern
```python
# Environment-aware configuration loading
CATALOG = spark.conf.get("CATALOG", "<TODO: your_default_catalog>")
SCHEMA = spark.conf.get("SCHEMA", "<TODO: your_default_schema>")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")

# TODO: Add your specific configuration parameters
# Example: DATA_SOURCE_PATH = spark.conf.get("DATA_SOURCE_PATH", "/path/to/your/data")
```

#### Critical Path Handling Pattern
```python
# CRITICAL: Include this pattern in all pipeline files
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # ... construct path from notebook context
except:
    # Fallback to multiple common paths
    possible_paths = ["/Workspace/src", "/databricks/driver/src"]
    for path in possible_paths:
        sys.path.insert(0, path)
```

#### Data Quality Expectation Patterns
```python
# TODO: Customize these expectations for your specific data quality requirements
@dlt.expect_all_or_drop({
    "valid_primary_key": "<TODO: your_primary_key_column> IS NOT NULL AND <TODO: your_primary_key_column> > 0",
    "valid_foreign_keys": "<TODO: your_foreign_key_constraints>"
})
@dlt.expect_all({
    "reasonable_values": "<TODO: your_business_logic_constraints>",
    "logical_relationships": "<TODO: your_data_consistency_rules>"
})
def your_table():
    return dlt.read("source_table")
```

### Common Pitfalls to Avoid
- **Manual deployment** instead of using Asset Bundles
- **Missing environment separation** in Asset Bundle configuration
- **Hard-coding workspace URLs** or environment-specific values in databricks.yml
- **Creating unused utility modules** - prefer Asset Bundle variables + `spark.conf.get()` pattern
- **Missing path handling** in pipeline files - include comprehensive path resolution
- **Skipping data quality expectations** - use `@dlt.expect_*` decorators consistently
- Hard-coding connection strings or credentials
- Ignoring data quality constraints in pipeline design
- Over-engineering synthetic data generation (start simple)
- Missing proper error handling in DLT pipelines
- Not considering data retention policies
- <TODO: Add specific pitfalls relevant to your business domain>

## NEXT STEPS

1. **Phase 0**: **Initialize and configure Databricks Asset Bundle structure**
   - Set up `databricks.yml` with dev/staging/prod environments
   - Configure workspace permissions and compute resources
   - Test basic deployment workflow
2. **Phase 1**: Set up synthetic data generation for <TODO: your core business entities>
3. **Phase 2**: Implement Bronze layer ingestion pipelines for <TODO: your data sources>
4. **Phase 3**: Build Silver layer transformation logic for <TODO: your data quality rules>
5. **Phase 4**: Create Gold layer dimensional models for <TODO: your business analytics>
6. **Phase 5**: Implement monitoring and alerting for <TODO: your SLA requirements>

## CONTRIBUTING

Before implementing features:
1. Create a PRP (Problem Requirements & Proposal) using templates in `PRPs/templates/`
2. Review existing examples for patterns and best practices
3. Consult documentation sources for latest recommendations
4. Test with synthetic data before production deployment
5. **<TODO: Add your specific contribution guidelines and approval processes>**

---

*This project template demonstrates modern data engineering practices using Databricks platform capabilities while maintaining enterprise-grade quality and performance standards for <TODO: your specific business domain>.*