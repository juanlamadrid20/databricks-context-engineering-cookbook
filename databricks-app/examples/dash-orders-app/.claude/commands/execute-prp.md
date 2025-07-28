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

3. **Safety Preparations**
   - Create feature branch: `git checkout -b feature/${ARGUMENTS}`
   - Backup current bundle configuration: `cp databricks.yml databricks.yml.backup.$(date +%Y%m%d_%H%M%S)`
   - Set error trap for automatic rollback on failure

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

### Phase 3: Completion & Documentation

1. **Final Validation**
   - Run complete validation suite from PRP
   - Verify all checklist items are completed
   - Ensure Unity Catalog governance compliance

2. **Success Documentation**
   - Generate implementation summary with app details
   - Document validation results and next steps
   - Create PR-ready summary for code review

## Error Handling & Rollback

### Automatic Rollback on Failure
```bash
cleanup_on_failure() {
    echo "=== FAILURE DETECTED - INITIATING ROLLBACK ==="
    
    # Restore bundle configuration
    if [ -f databricks.yml.backup.* ]; then
        BACKUP_FILE=$(ls -t databricks.yml.backup.* | head -1)
        cp ${BACKUP_FILE} databricks.yml
        echo "Restored databricks.yml from ${BACKUP_FILE}"
    fi
    
    # Destroy development deployment
    databricks bundle destroy --target dev --auto-approve || echo "No dev deployment to clean up"
    
    # Return to main branch and cleanup
    git checkout main || git checkout master
    git branch -D feature/${ARGUMENTS} 2>/dev/null || echo "Feature branch cleanup skipped"
    
    echo "Rollback completed"
    exit 1
}

# Set trap for automatic error handling
trap cleanup_on_failure ERR
```

### Success Completion
```bash
# Create comprehensive implementation summary
cat > "PRPs/${ARGUMENTS}_execution_summary.md" << EOF
# ${ARGUMENTS} Implementation Summary

## Execution Date
$(date)

## App Details
- App ID: ${APP_ID}
- Target Environment: dev
- Git Branch: feature/${ARGUMENTS}

## Validation Results
- Configuration: ✓ Passed
- Deployment: ✓ Passed  

## Next Steps  
1. Deploy to staging: \`databricks bundle deploy --target staging\`
2. Create PR for code review

## Resources Created
- Unity Catalog Tables: Check \${CATALOG}.\${SCHEMA}.*
- Databricks App: ${APP_ID}
- Asset Bundle Resources: See databricks.yml updates
EOF

echo "🎉 PRP execution completed successfully!"
echo "Implementation summary: PRPs/${ARGUMENTS}_execution_summary.md"
```

## Quality Gates & Success Criteria

### Mandatory Validation Gates
- [ ] Asset Bundle validates without errors
- [ ] All Python files compile successfully
- [ ] App deploys to dev environment

### Rollback Triggers
- Configuration validation failure
- Deployment errors or timeouts
- Manual interrupt (Ctrl+C)

This enhanced execution framework provides production-ready implementation of PRPs with comprehensive validation, monitoring, and rollback capabilities for reliable Databricks development.