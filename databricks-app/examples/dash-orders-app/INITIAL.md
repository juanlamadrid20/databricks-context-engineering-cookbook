# Databricks App Project

## PROJECT OVERVIEW

This project implements a Databricks App reading and editing data in Lakebase using a spreadsheet style interface.

### Core Technologies
- **Platform**: Databricks Apps
- **Data Source**: Lakebase (Postgres)
- **Frontend**: Dash (Python-based web framework)
- **Backend**: Python with SQLAlchemy and psycopg3 for Postgres connectivity. Do not use ORM modules.
- **Database**: Postgres tables in Databricks Lakebase
- **Dataset**: Retail360 order data (dbdemos.ai schema)
- **Analytics**: Databricks Genie space for aggregated order analytics

### Key Features
- Multi-view Dash interface for order management:
  - **Spreadsheet View**: Bulk order editing with DataTable
  - **Single Order Form**: Detailed order editing interface
  - **Genie Analytics View**: Aggregated today's orders analytics
- Real-time data reading from Lakebase Postgres orders tables
- In-place order editing capabilities via spreadsheet and form
- Batch operations support for bulk order updates
- Databricks Genie integration for order analytics and insights

## ARCHITECTURE

### Application Structure
```
databricks-app/
├── app.py                    # Main Dash application entry point
├── config.py                 # Configuration settings
├── requirements.txt          # Python dependencies
├── project.toml             # Project metadata
│
├── pages/              # Dash UI components
│   ├── __init__.py
│   ├── spreadsheet.py       # Orders DataTable component
│   ├── order_form.py        # Single order form component
│   └── genie_view.py        # Genie analytics component
│
├── utils/                     # Core application libraries
│   ├── __init__.py
│   ├── database.py          # Database connection and queries
│   └── app_utils.py             # Helper functions
│
└── setup/                   # Setup and initialization scripts
    ├── __init__.py
    ├── setup_database.py    # Database setup and schema creation
    ├── populate_data.py     # Data population script
    ├── setup_genie.py       # Genie space configuration
    └── schema.sql           # Database schema definition
```

## IMPLEMENTATION DETAILS

### 1. DATABASE LAYER (`utils/database.py`)

**Connection Management:**
- Implement connection managment use SQLAlechmy and psycopg3 like in the following example, but provide option to pass in username and password or use the current user.

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

### 2. CONFIGURATION (`config.py`)

**Application Settings:**
- Pagination limits (default: 100 rows per page)
- Connection pool settings (min: 5, max: 20 connections)
- Session timeout configuration
- Error retry attempts and delays

### 3. UI COMPONENTS

#### Spreadsheet Component (`pages/spreadsheet.py`)
**Features:**
- Editable DataTable with column sorting/filtering
- Inline cell editing with validation
- Bulk selection and batch operations
- Export to CSV functionality
- Real-time data refresh capabilities

#### Order Form Component (`pages/order_form.py`)
**Form Fields:**
- Order ID (read-only)
- Customer information (name, email, address)
- Product details (SKU, quantity, price)
- Order status dropdown
- Timestamps (created, modified)

**Validation Rules:**
- Required field validation
- Email format validation
- Quantity/price numeric validation
- Status transition rules

#### Genie View Component (`pages/genie_view.py`)
**Analytics Widgets:**
- Today's order count and revenue
- Top products by quantity/revenue
- Order status distribution
- Hourly order trends
- Customer acquisition metrics

### 4. MAIN APPLICATION (`app.py`)

**Page Structure:**
```python
# Multi-page layout using Dash Pages
dash.register_page("spreadsheet", path="/", layout=spreadsheet_layout)
dash.register_page("order-form", path="/order/<order_id>", layout=form_layout)
dash.register_page("analytics", path="/analytics", layout=genie_layout)
```

**Global Components:**
- Navigation sidebar with page links
- Error message display system
- Loading indicators for async operations
- Session state management

### 5. SETUP SCRIPTS

#### Database Setup (`setup/setup_database.py`)
**Tasks:**
- Create Lakebase connection
- Execute schema.sql to create tables
- Set up indexes for performance
- Create database users and permissions
- Validate connection and table structure

#### Data Population (`setup/populate_data.py`)
**Data Generation:**
- Generate realistic retail order data
- Create customers, products, and orders
- Populate with various order statuses
- Add timestamp variations for testing
- Insert sample data (1000+ orders recommended)

#### Genie Setup (`setup/setup_genie.py`)
**Configuration:**
- Create Genie space for order analytics
- Configure data connections to order tables
- Set up pre-built analytics queries
- Create shareable dashboard links
- Test Genie API integration

### 6. DEPLOYMENT REQUIREMENTS

#### Dependencies (`requirements.txt`)
```
dash>=2.15.0
dash-bootstrap-components>=1.5.0
plotly>=5.17.0
psycopg[binary]>=3.1.0
sqlalchemy>=2.0.0
pandas>=2.1.0
python-dotenv>=1.0.0
databricks-sdk>=0.58.0
```

#### Project Configuration (`project.toml`)
Add a project.toml file for the app named databricks-orders-app.

### 7. ERROR HANDLING & LOGGING

**Error Management:**
- Database connection error recovery
- Data validation error messages
- User-friendly error displays
- Logging to Databricks logs
- Graceful degradation for Genie failures
- Allow app to run if Genie info not provided

### 8. PERFORMANCE CONSIDERATIONS

**Optimization Strategies:**
- Implement data pagination for large datasets
- Cache frequently accessed data
- Lazy loading for analytics components
- Database query optimization with proper indexes

### 9. SECURITY MEASURES

**Data Protection:**
- Input sanitization for all form fields
- SQL injection prevention
- User session management
- Role-based access controls (if needed)
- Secure credential management

### 10. TESTING STRATEGY

**Test Categories:**
- Database connection and query tests
- UI component unit tests
- End-to-end workflow testing
- Data validation testing
- Performance load testing

### Retail360 Orders Data Schema
- **Orders**: Core order details (order_id, customer_id, order_date, status, total_amount)
- **Order_Items**: Line items (order_id, product_id, quantity, unit_price, subtotal)
- **Products**: Product catalog for order items
- **Customers**: Customer information for order context
- **Order_Status_History**: Order status change tracking


## DEPLOYMENT APPROACH

**This project MUST use Databricks Asset Bundles for all deployment and management activities.**

### Why Asset Bundles?
- **Infrastructure-as-Code**: Version-controlled deployment configurations
- **Environment Management**: Seamless promotion between dev/staging/prod
- **Declarative Configuration**: YAML-based configuration for reproducible deployments
- **Best Practice**: Recommended approach for modern Databricks project management

## DEVELOPMENT PHASES

### Phase 1: Core Infrastructure
- Set up Databricks App framework with multi-page Dash layout
- Implement SQLAlchemy + psycopg3 connection pool
- Create basic Dash application structure with navigation
- Set up Retail360 orders data schema and sample data
- Create Databricks Genie space for order analytics

### Phase 2: Orders Management Interface
- Build orders spreadsheet DataTable with SQLAlchemy integration
- Implement single order form with validation
- Add order editing functionality with callbacks
- Create order filtering and search capabilities
- Implement navigation between views

### Phase 3: Genie Analytics Integration
- Integrate Databricks Genie space for order analytics
- Create today's orders aggregated view
- Add real-time analytics dashboard
- Implement seamless switching between editing and analytics views

### Phase 4: Advanced Features
- Batch operations and SQLAlchemy transaction support
- Advanced order analytics and reporting
- Export/import capabilities for order data
- Audit logging and change tracking for order modifications
- Performance optimization for connection pool

### Phase 5: Production Readiness
- Connection pool tuning and monitoring
- Error handling and recovery for database operations
- Security implementation for order data protection
- Documentation and testing for retail360 order workflows

## CONFIGURATION

### Environment Variables
```
DATABRICKS_HOST=<workspace-url>
DATABRICKS_PROFILE=<access-token> # (Optional) If passed, use in the WorkspaceClient init
POSTGRES_HOST=<lakebase-postgres-host>
POSTGRES_PORT=5432
POSTGRES_DB=<lakebase-database>
POSTGRES_USER=<username> # (Optional) If not passed, use WorkspaceClient to get the current user name
POSTGRES_PASSWORD=<password> # (Optional) If passed, skip creating credentials with WorkspaceClient and use this password
RETAIL360_SCHEMA=<schema-name>
GENIE_SPACE_ID=<genie-space-id> # If not passed, avoid failure and show error mesage on the genie page
GENIE_API_ENDPOINT=<genie-api-url> # If not passed, avoid failure and show error mesage on the genie page
```

### Required Dependencies
```
dash>=2.14.0
dash-bootstrap-components>=1.5.0
sqlalchemy>=2.0.0
psycopg3>=3.1.0
pandas>=2.0.0
plotly>=5.15.0
databricks-sdk>=0.18.0
```

### Required Permissions
- Read/Write access to Lakebase Postgres orders tables
- Databricks App deployment permissions
- Postgres connection permissions for retail360 schema
- Databricks Genie space creation and query permissions
- SQLAlchemy connection pool management permissions

## SUCCESS CRITERIA

- Users can view and edit Retail360 orders data in multiple interfaces:
  - Spreadsheet view for bulk order management
  - Single order form for detailed editing
  - Genie analytics view for today's aggregated order insights
- Real-time editing of orders with immediate visual feedback
- Order data changes persist reliably via SQLAlchemy transactions
- Seamless navigation between editing and analytics views
- Proper handling of retail360 orders schema constraints and relationships
- Today's order analytics provided through Databricks Genie integration

## TECHNICAL CONSTRAINTS

- Must work within Databricks Apps framework
- Respect Lakebase data governance policies
- Handle concurrent user access to order data via connection pooling
- Maintain order data consistency during transactions
- Support standard spreadsheet operations (sort, filter, format)
- Comply with retail360 orders schema and relationships
- Ensure order data privacy and security
- Genie space must reflect same data structure as editable orders
- Real-time sync between editing views and analytics

## NEXT STEPS

1. Initialize Databricks App project with multi-page Dash structure
2. Set up SQLAlchemy + psycopg3 connection pool and test retail360 orders data access
3. Create Databricks Genie space for order analytics using same data source
4. Implement basic orders spreadsheet DataTable with SQLAlchemy integration
5. Add single order form with validation and SQLAlchemy session management
6. Create Genie analytics view for today's order aggregations
7. Implement navigation between editing and analytics views
8. Generate or import retail360 orders sample data matching dbdemos.ai schema

## EXAMPLES & REFERENCES

### Code Examples
- [Retail 360 demos](https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/c360-platform-reduce-churn?itm_data=demo_center)

## DOCUMENTATION

### Primary Documentation Sources
- **Context7 MCP Server**: For best practices and updated documentation
- **Databricks Documentation**:
  - [Databricks Lakebase](https://docs.databricks.com/aws/en/oltp/query/)
  - [Databricks Apps Guide](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
  - [Databricks Apps Cookbook](https://apps-cookbook.dev/)

### Additional Resources
- **Data Governance**: Unity Catalog policies for PII handling in user data
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Medallion Architecture Best Practices](https://www.databricks.com/glossary/medallion-architecture)
- [Databricks App Code examples](https://github.com/databricks/app-templates)

## DEVELOPMENT GUIDELINES

### Schema Implementation
- **Naming Convention**: snake_case for all tables and columns, prefixed by layer (bronze_, silver_, gold_)
- **Data Type Standards**: STRING for IDs, DOUBLE for monetary amounts, DATE for date fields, TIMESTAMP for audit fields
- **Constraint Patterns**: NOT NULL for business keys, CHECK constraints for stage progressions and valid enum values


### Best Practices
1. **Asset Bundle First**: Always use Asset Bundles for deployment - never deploy manually
2. **Environment Isolation**: Use separate Asset Bundle targets for dev/staging/prod
3. **Version Control**: All databricks.yml configurations must be version controlled
4. **Configuration via Variables**: Use Asset Bundle variables
7. **Testing**: Include unit tests for data generation logic
8. **Documentation**: Document code for users and for developers.


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


### Common Pitfalls to Avoid
- **Manual deployment** instead of using Asset Bundles
- **Missing environment separation** in Asset Bundle configuration
- **Hard-coding workspace URLs** or environment-specific values in databricks.yml
- **Creating unused utility modules** - prefer Asset Bundle variables
- Hard-coding connection strings or credentials
- Over-engineering synthetic data generation (start simple)
- Splitting code into too many folders and files (this should be a simple codebase)


## CONTRIBUTING

Before implementing features:
1. Create a PRP (Problem Requirements & Proposal) using templates in `PRPs/templates/`
2. Review existing examples for patterns and best practices
3. Consult documentation sources for latest recommendations
4. Test with synthetic data before production deployment
5. **Data Privacy Compliance**: Ensure PII handling follows company data governance policies

---

*This project template demonstrates a Databricks App to interact with data within your Databricks Workspace.*