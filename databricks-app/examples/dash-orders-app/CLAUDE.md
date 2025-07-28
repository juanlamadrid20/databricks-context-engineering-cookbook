# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks App project using Dash to interact with Lakebase and Genie. It uses Asset Bundles for deployment management.

## Consolidated Documentation References Hub

### External Documentation
Use these documentation sources as the single source of truth for all development activities.

```yaml
# Primary Databricks Documentation
databricks_core:
  asset_bundles:
    - url: https://docs.databricks.com/dev-tools/bundles/index.html
      usage: "Asset Bundle configuration, deployment patterns, and environment management"
    - url: https://docs.databricks.com/dev-tools/cli/bundle-cli.html
      usage: "Asset Bundle CLI commands and workflow automation"

  databricks_apps:
    - url: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/
      usage: "General Databricks App configuration, patterns, code examples, and environment management"
    - url: https://apps-cookbook.dev/
      usage" "Databricks App example code"
    
  lakebase:
    - url: https://docs.databricks.com/aws/en/oltp/query/
      usage: "General Databricks Lakebase guidance, configuration and auth patterns"
      
  unity_catalog:
    - url: https://docs.databricks.com/data-governance/unity-catalog/best-practices.html
      usage: "Three-part naming conventions, governance patterns, and PII handling"
    - url: https://docs.databricks.com/data-governance/unity-catalog/index.html
      usage: "Unity Catalog setup, permissions, and data governance framework"
      
  performance_optimization:
    - url: https://docs.databricks.com/delta/optimize.html
      usage: "Delta Lake optimization, Z-ordering, and liquid clustering strategies"

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

## Architecture Overview

### Data Engineering Patterns
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)

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

### Databricks App Dash Patterns

#### Connection Management

- Implement connection managment use SQLAlechmy and psycopg3, like in the following example, but provide option to pass in username and password or use the current user.

```python
from sqlalchemy import create_engine, text, event
from databricks.sdk import WorkspaceClient
import os
import uuid
import time

global host, instance_name, database_name
instance_name = os.getenv("LAKEBASE_INSTANCE_NAME", "")
database_name = os.getenv("LAKEBASE_DATABASE_NAME", "databricks_postgres")

# Initialize Databricks SDK client
w = WorkspaceClient()

instance = w.database.get_database_instance(name=instance_name)

username = w.current_user.me().user_name
host = instance.read_write_dns
port = 5432
database = database_name

# sqlalchemy setup + function to refresh the OAuth token that is used as the Postgres password every 15 minutes.
connection_pool = create_engine(f"postgresql+psycopg://{username}:@{host}:{port}/{database}")
postgres_password = None
last_password_refresh = time.time()

@event.listens_for(connection_pool, "do_connect")
def provide_token(dialect, conn_rec, cargs, cparams):
    global postgres_password, last_password_refresh, host

    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing PostgreSQL OAuth token")
        cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])
        postgres_password = cred.token
        last_password_refresh = time.time()

    cparams["password"] = postgres_password

with connection_pool.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    for row in result:
        print(f"Connected to PostgreSQL database. Version: {row}")
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

### Proven Configuration Patterns
#### Asset Bundle Variable Management
```yaml
variables:
  catalog:
    description: "Unity Catalog name"
    default: "juan_dev"
  schema:
    description: "Schema name for your data"
    default: "sales_ops"
  max_files_per_trigger:
    description: "Performance tuning parameter"
    default: 100

targets:
  dev:
    variables:
      catalog: "juan_dev"
      schema: "sales_ops"
      max_files_per_trigger: 100
  prod:
    variables:
      catalog: "juan_prod"
      schema: "sales_ops"
      max_files_per_trigger: 1000
```

### Planning Process
Before implementing features:
1. Create a PRP using templates in `PRPs/templates/prp_base.md`
2. Review `INITIAL.md` for project-specific requirements and patterns
3. Use the comprehensive PRP template which includes validation loops and context requirements
4. Follow the Asset Bundle deployment workflow

### Common Pitfalls to Avoid
- **Manual deployment** instead of using Asset Bundles
- **Missing environment separation** in Asset Bundle configuration
- **Hard-coding workspace URLs** or environment-specific values in databricks.yml
- **Creating unused utility modules** - prefer Asset Bundle variables
- Hard-coding connection strings or credentials
- Over-engineering synthetic data generation (start simple)
- Splitting code into too many folders and files (this should be a simple codebase)

### Testing Strategy
1. **Syntax Validation**: `databricks bundle validate --target dev`
2. **Unit Tests**: Test python functinality using pytest
3. **Integration Tests**: Deploy to dev environment and validate end-to-end app usage

#### Essential Test Commands
```bash
# Complete validation workflow
databricks bundle validate --target dev
databricks apps create <app_name>
databricks sync . /Workspace/Users/<user_name>/<app_name>
databricks apps deploy <app_name> --source-code-path /Workspace/Users/<user_name>/<app_name>
databricks bundle deploy --target dev

# Unity Catalog validation
databricks catalogs get <catalog_name>
databricks schemas list --catalog-name <catalog>
databricks tables list --catalog-name <catalog> --schema-name <schema>
```

## Key Files and Their Purpose

- `INITIAL.md` - Comprehensive project documentation and setup guide
- `PRPs/prp_base.md` - Template for systematic feature planning with AI agents
- `databricks.yml` - Asset Bundle configuration (to be created)

*This project template demonstrates a Databricks App to interact with data within your Databricks Workspace.*