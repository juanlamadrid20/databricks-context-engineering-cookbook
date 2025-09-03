# About
This project focuses on **Context Engineering** methods to build solutions on Databricks.  
It provides practical examples and solution patterns demonstrating how context engineering methods can be applied using the Databricks platform.

**At the end of this doc is a quick start section on how to get started.**

These examples are designed to accelerate development, promote best practices, and showcase how context engineering concepts can be operationalized at scale with Databricks.

# About Context Engineering

Context engineering is the discipline of designing dynamic systems that assemble and deliver precisely the right information, tools, and instructions—in the optimal format and at the right moment—so that large language models (LLMs) or generative AI can effectively accomplish complex tasks. Unlike prompt engineering, which focuses narrowly on crafting an input statement, context engineering creates a workflow that integrates relevant data, prior conversations, external knowledge, and tool outputs, ensuring that the AI receives all necessary context to reason accurately, adapt to evolving needs, and deliver reliable results

This approach is especially powerful on Databricks, where unified analytics, machine learning, and application frameworks converge on a single Lakehouse platform.


# About this repo and project structure. 

This repository is organized into a hierarchical structure that reflects the different levels of context engineering solutions:

## Repository Structure

### Technical Domains
The repository is organized around main technical domains such as:
- **data-eng** - Data engineering solutions and patterns
- **data-apps** - Databricks Application development patterns (TBD)
- And other domain-specific areas

### Pattern Organization
Under each technical domain, there are several solution patterns. For example:
- **data-eng/declarative-pipelines** - Declarative pipeline patterns for data engineering

### Pattern Structure
Each pattern follows a consistent structure with two main folders:

#### Foundations
The `foundations/` folder contains generic reusable **PRP (Product Requirement Prompt) templates**. These are the building blocks and reference implementations that can be applied across different use cases. They typically contain directions and
todos to fill out for your own implementations.

#### Implementations
The `implementations/` folder contains fully realized sample projects with full PRP files** with complete example code. These are concrete implementations that demonstrate how to apply the foundational patterns to specific scenarios.

This structure allows developers to:
- Start with foundational templates for understanding core concepts
- Reference complete implementations for practical application
- Build upon established patterns for their own solutions

## Hierarchical Tree Representation

```
databricks-context-engineering-cookbook/
├── data-eng/                                               <technical domain>
│   └── declarative-pipelines/                              <a technical domain pattern>
│       ├── foundations/                                    <foundational PRP files for this domain(data-eng) and pattern (LDP)>
│       │   ├── CLAUDE.md                                   <Claude AI guidelines and context for this pattern>
│       │   ├── .claude/                                    <Claude AI slash commands used in claude code>
│       │   └── PRPs/                                       <Product Requirement Prompt templates>
│       │       └── ldp_medallion_base.md                   <base PRP template for this pattern>
│       └── implementations/                                <1 to many example implementations for this domain and pattern>
│           └── healthcare-ldp-medallion/                   <specific implementation of a fictitious HLS use case>
│               ├── README.md
│               ├── CLAUDE.md                               <Claude AI guidelines for this specific implementation>
│               ├── databricks.yml
│               ├── PRPs/                                   <Product Requirement Prompts for this implementation>
│               │   ├── ldp_medallion_hls_base.md
│               │   └── patient_data_medallion_pipeline.md
│               ├── resources/
│               │   ├── jobs.yml
│               │   └── pipelines.yml
│               ├── src/
│               │   ├── jobs/
│               │   │   └── data_generation/
│               │   │       ├── csv_file_writer.py
│               │   │       ├── synthetic_data_notebook.py
│               │   │       └── synthetic_patient_generator.py
│               │   ├── pipelines/
│               │   │   ├── bronze/
│               │   │   │   ├── bronze_claims.py
│               │   │   │   ├── bronze_medical_events.py
│               │   │   │   └── bronze_patients.py
│               │   │   ├── silver/
│               │   │   │   └── silver_patients.py
│               │   │   ├── gold/
│               │   │   ├── identity_resolution/
│               │   │   └── shared/
│               │   │       └── healthcare_schemas.py
│               │   └── tests/
│               └── claude_md_files/
│                   └── CLAUDE-PYTHON-BASIC.md
└── [future domains]/
    └── [future patterns]/
        ├── foundations/
        └── implementations/
```

### Tree Structure Explanation

- **Root Level**: Main repository with overview documentation
- **Technical Domains** (e.g., `data-eng/`): High-level solution areas
- **Patterns** (e.g., `declarative-pipelines/`): Specific solution approaches within each domain
- **Foundations**: Reusable PRP templates and building blocks
  - **CLAUDE.md**: Claude AI guidelines and context for the pattern
  - **.claude/**: Claude AI configuration and context files
  - **PRPs/**: Product Requirement Prompt templates
- **Implementations**: Complete working examples with full code and documentation
- **Future Expansion**: Placeholder structure for additional domains and patterns


# Quick Start

## Getting Started

1. **Clone this repository**
   ```bash
   git clone <repository-url>
   cd databricks-context-engineering-cookbook
   ```

2. **Choose your technical domain**
   - Currently available: **data-eng** (Data engineering solutions and patterns)
   - Navigate to your domain of interest: `cd data-eng/`

3. **Select your pattern**
   - Currently available: **declarative-pipelines** (Declarative pipeline patterns)
   - Navigate to the pattern: `cd declarative-pipelines/`

4. **Use the foundations PRP scaffolding**
   - Start with the foundational templates in the `foundations/` folder
   - These contain reusable PRP (Product Requirement Prompt) templates with directions and TODOs to fill out for your specific use case

5. **Customize for your use case**
   - Edit `CLAUDE.md` with your specific guidelines and requirements
   - Modify the base PRP file (e.g., `ldp_medallion_base.md`) to add your specific use case requirements
   - Reference the `implementations/` section for complete working examples

6. **Generate and execute your PRP**
   - Use the slash commands to generate your PRP:
     ```
     /prp-base-create ldp_medallion_base.md
     ```
   - Execute your PRP to generate code:
     ```
     /prp-base-execute PRPs/your-feature.md
     ```

## Example Workflow

1. Clone the repository
2. Navigate to `data-eng/declarative-pipelines/foundations/`
3. Review the base PRP template (`ldp_medallion_base.md`)
4. Create your own implementation folder
5. Copy and customize `CLAUDE.md` and the base PRP for your specific requirements
6. Use slash commands to generate and execute your PRP
7. Reference `implementations/healthcare-ldp-medallion/` for a complete working example
