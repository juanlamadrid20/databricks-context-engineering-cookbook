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
