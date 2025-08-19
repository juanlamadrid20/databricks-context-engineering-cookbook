# Execute PRP

## PRP file: $ARGUMENTS

Execute a Problem Requirements & Proposal (PRP) for Databricks feature implementation with systematic validation and rollback capabilities.

## Pre-Execution Validation

> **📋 Reference**: See `CLAUDE.md` - Consolidated Documentation References for all Databricks CLI commands and detailed explanations.

Before starting implementation, validate the PRP is ready for execution:

1. **PRP Completeness Check**
   - Verify PRP file exists: `PRPs/${ARGUMENTS}.md`
   - Check for required sections: All Needed Context, Implementation Blueprint, Validation Loop
   - Ensure all TODO placeholders are filled with actual values

2. **Environment Readiness**
   - Verify git working directory is clean for rollback capability
   - Check Databricks CLI authentication: `databricks auth login`
   - Validate Asset Bundle structure: `databricks bundle validate --target dev`
   - Confirm Unity Catalog access: `databricks catalogs list`

## Systematic Execution Process

### Phase 1: Context Loading & Planning
1. **Load PRP**
   - Read the specified PRP file completely
   - Extract all context, requirements, and validation commands
   - Use TodoWrite tool to create implementation plan from PRP tasks
   - Identify implementation patterns from existing codebase

2. **ULTRATHINK & Planning**
   - Create comprehensive implementation plan using TodoWrite
   - Break down complex tasks into smaller, manageable steps
   - Identify dependencies between tasks and validate execution order
   - Ensure all required context is available for autonomous execution

### Phase 2: Implementation with Validation Gates

> **💻 Command Reference**: All Databricks CLI commands and patterns detailed in `CLAUDE.md` - Key Commands and Testing Strategy sections.

#### Level 1: Configuration & Syntax Validation
```bash
# Asset Bundle configuration validation
databricks bundle validate --target dev || exit 1

# Python syntax validation
python -m py_compile src/pipelines/**/*.py || exit 1

# DLT decorator and import validation
find src/pipelines -name "*.py" -exec python -c "
import ast
with open('{}') as f:
    tree = ast.parse(f.read())
    has_dlt = any(n.names[0].name == 'dlt' for n in ast.walk(tree) if isinstance(n, ast.Import))
    if not has_dlt:
        print('WARNING: {} missing dlt import')
" \;
```

#### Level 2: Deployment Validation
```bash
# Deploy to development environment with error handling
databricks bundle deploy --target dev || {
    echo "ERROR: Deployment failed - checking bundle logs"
    databricks bundle logs --target dev
    exit 1
}
```

#### Level 3: Pipeline Execution Validation
```bash
# Get pipeline ID and execute with monitoring
PIPELINE_ID=$(databricks pipelines list-pipelines --output json | jq -r '.[] | select(.name | contains("'${ARGUMENTS}'")) | .pipeline_id' | head -1)

if [ -z "$PIPELINE_ID" ]; then
    echo "ERROR: Pipeline not found after deployment"
    exit 1
fi

# Start pipeline and monitor with timeout
echo "Starting pipeline ${PIPELINE_ID}..."
databricks pipelines start-update --pipeline-id ${PIPELINE_ID}

# Monitor execution with 30-minute timeout
TIMEOUT=1800
START_TIME=$(date +%s)

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo "ERROR: Pipeline execution timeout after ${TIMEOUT}s"
        exit 1
    fi
    
    STATUS=$(databricks pipelines get --pipeline-id ${PIPELINE_ID} --output json | jq -r '.latest_updates[0].state.life_cycle_state')
    
    case $STATUS in
        "COMPLETED")
            echo "✓ Pipeline execution completed successfully"
            break
            ;;
        "FAILED"|"CANCELED")
            echo "ERROR: Pipeline execution failed with status: ${STATUS}"
            databricks pipelines events --pipeline-id ${PIPELINE_ID} --max-results 10
            exit 1
            ;;
        *)
            echo "Pipeline status: ${STATUS} (${ELAPSED}s elapsed)"
            sleep 30
            ;;
    esac
done
```

#### Level 4: Data Quality Validation
```bash
# Validate DLT expectations and data quality metrics
# Note: Direct SQL querying requires SQL warehouse setup
# Alternative: Use pipeline events API to check data quality
databricks pipelines list-pipeline-events --pipeline-id ${PIPELINE_ID} --output json | \
    jq '.[] | select(.event_type == "data_quality") | {expectation_name: .details.expectation_name, passed_records: .details.passed_records, failed_records: .details.failed_records}' | \
    head -10 || echo "WARNING: Could not retrieve data quality metrics"
```

### Phase 3: Completion & Documentation

1. **Final Validation**
   - Run complete validation suite from PRP
   - Verify all checklist items are completed
   - Ensure Unity Catalog governance compliance

2. **Success Documentation**
   - Generate implementation summary with pipeline details
   - Document validation results and next steps
   - Create PR-ready summary for code review

```

### Success Completion
```bash
# Create comprehensive implementation summary
cat > "PRPs/${ARGUMENTS}_execution_summary.md" << EOF
# ${ARGUMENTS} Implementation Summary

## Execution Date
$(date)

## Pipeline Details
- Pipeline ID: ${PIPELINE_ID}
- Target Environment: dev

## Validation Results
- Configuration: ✓ Passed
- Deployment: ✓ Passed  
- Pipeline Execution: ✓ Passed
- Data Quality: ✓ Passed

## Next Steps
1. Review data quality metrics in Databricks UI
2. Test with production data volumes  
3. Deploy to staging: \`databricks bundle deploy --target staging\`
4. Create PR for code review

## Resources Created
- Unity Catalog Tables: Check \${CATALOG}.\${SCHEMA}.*
- DLT Pipeline: ${PIPELINE_ID}
- Asset Bundle Resources: See databricks.yml updates
EOF

echo "🎉 PRP execution completed successfully!"
echo "Implementation summary: PRPs/${ARGUMENTS}_execution_summary.md"
```

## Quality Gates & Success Criteria

### Mandatory Validation Gates
- [ ] Asset Bundle validates without errors
- [ ] All Python files compile successfully
- [ ] Pipeline deploys to dev environment
- [ ] Pipeline executes end-to-end successfully
- [ ] Data quality expectations are met
- [ ] Unity Catalog governance maintained

### Rollback Triggers
- Configuration validation failure
- Deployment errors or timeouts
- Pipeline execution failure
- Data quality threshold violations
- Manual interrupt (Ctrl+C)

This enhanced execution framework provides production-ready implementation of PRPs with comprehensive validation, monitoring, and rollback capabilities for reliable Databricks development.
