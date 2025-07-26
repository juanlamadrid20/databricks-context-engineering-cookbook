# About 
This repository provides a modern data engineering project template for Databricks, built on context engineering principles. It features:

- Medallion architecture implementation (Bronze → Silver → Gold layers)
- Declarative data pipelines using Delta Live Tables (DLT)
- Infrastructure-as-code deployment with Databricks Asset Bundles
- Best practices for data quality, governance and CI/CD
- Comprehensive documentation and example implementations

The template enables rapid development of production-ready Databricks solutions while enforcing architectural standards and deployment best practices.


# File Responsibility Comparison Table

| Attribute | @CLAUDE.md | @INITIAL.md | @prp_base.md | @execute-prp.md | @generate-prp.md |
|-----------|------------|-------------|--------------|-----------------|------------------|
| **Main Responsibility** | Technical development guide & patterns | Project overview & business context | Feature planning template | PRP execution framework | PRP creation process |
| **Primary Purpose** | "How to implement" | "What to build" | "Plan a feature" | "Execute a plan" | "Create a plan" |
| **Target Audience** | Developers & AI agents | Product/Business stakeholders | Feature planners | Implementation teams | Planning teams |

## Content Guidelines

### **What SHOULD be included:**

| File | Should Include |
|------|----------------|
| **@CLAUDE.md** | • Asset Bundle patterns<br>• DLT pipeline examples<br>• Configuration templates<br>• Development workflows<br>• External documentation links<br>• Critical code patterns<br>• Environment setup |
| **@INITIAL.md** | • Business domain context<br>• Entity schemas<br>• Project architecture<br>• Success criteria<br>• Compliance requirements<br>• Data governance<br>• Domain-specific examples |
| **@prp_base.md** | • Feature planning template<br>• Implementation blueprints<br>• Validation loops<br>• Context requirements<br>• Test cases<br>• Anti-patterns<br>• Domain-specific code examples |
| **@execute-prp.md** | • Execution commands<br>• Validation gates<br>• Error handling<br>• Rollback procedures<br>• Monitoring scripts<br>• Success criteria<br>• Quality gates |
| **@generate-prp.md** | • Research strategies<br>• Context gathering<br>• PRP creation workflow<br>• Quality scoring<br>• Template population<br>• Validation criteria |

### **What should NOT be included:**

| File | Should NOT Include |
|------|-------------------|
| **@CLAUDE.md** | ❌ Business requirements<br>❌ Specific feature plans<br>❌ Execution commands<br>❌ Domain schemas<br>❌ Project roadmaps |
| **@INITIAL.md** | ❌ Implementation code<br>❌ Technical patterns<br>❌ Execution steps<br>❌ Development workflows<br>❌ CLI commands |
| **@prp_base.md** | ❌ General development patterns<br>❌ Project setup instructions<br>❌ Execution frameworks<br>❌ Non-feature content |
| **@execute-prp.md** | ❌ Planning templates<br>❌ Business requirements<br>❌ General development guides<br>❌ Feature specifications |
| **@generate-prp.md** | ❌ Actual implementation code<br>❌ Execution commands<br>❌ Business requirements<br>❌ Technical patterns |

## Key Attributes

### **Usage Patterns:**

| File | When to Use | Update Frequency |
|------|-------------|------------------|
| **@CLAUDE.md** | Before any development work | Rarely (foundational patterns) |
| **@INITIAL.md** | Project onboarding & context setting | Occasionally (major project changes) |
| **@prp_base.md** | Planning new features | Never (it's a template) |
| **@execute-prp.md** | Implementing planned features | Rarely (process improvements) |
| **@generate-prp.md** | Creating feature plans | Rarely (process improvements) |

### **Information Density:**

| File | Content Type | Detail Level | Context Richness |
|------|-------------|--------------|------------------|
| **@CLAUDE.md** | Technical patterns | High | Implementation-focused |
| **@INITIAL.md** | Business context | Medium | Domain-focused |
| **@prp_base.md** | Planning framework | Very High | Feature-specific |
| **@execute-prp.md** | Process framework | High | Execution-focused |
| **@generate-prp.md** | Meta-process | Medium | Process-focused |

### **Dependencies & Relationships:**

| File | Depends On | Feeds Into | Relationship |
|------|------------|------------|--------------|
| **@CLAUDE.md** | External docs, best practices | All implementation work | Foundation |
| **@INITIAL.md** | Business requirements | Feature planning | Context provider |
| **@prp_base.md** | @CLAUDE.md + @INITIAL.md | Feature implementation | Planning bridge |
| **@execute-prp.md** | Completed PRPs | Working features | Execution engine |
| **@generate-prp.md** | @CLAUDE.md + codebase analysis | New PRPs | Planning catalyst |

### **Maintenance Characteristics:**

| File | Maintenance Burden | Version Control Priority | Stability |
|------|-------------------|-------------------------|-----------|
| **@CLAUDE.md** | Low (stable patterns) | High (affects all dev) | Very Stable |
| **@INITIAL.md** | Medium (business evolution) | High (project foundation) | Stable |
| **@prp_base.md** | None (template) | Medium (process improvement) | Very Stable |
| **@execute-prp.md** | Low (process refinement) | Medium (execution quality) | Stable |
| **@generate-prp.md** | Low (process refinement) | Low (meta-process) | Stable |

## **Context Engineering Perspective:**

### **For AI Agent Usage:**

| File | AI Agent Value | Context Engineering Role |
|------|----------------|------------------------|
| **@CLAUDE.md** | ⭐⭐⭐⭐⭐ | Implementation patterns & technical context |
| **@INITIAL.md** | ⭐⭐⭐⭐ | Business domain & requirement context |
| **@prp_base.md** | ⭐⭐⭐⭐⭐ | Complete feature context & planning framework |
| **@execute-prp.md** | ⭐⭐⭐ | Execution validation & quality assurance |
| **@generate-prp.md** | ⭐⭐ | Meta-planning & context gathering strategy |

### **Optimal Usage Strategy:**
1. **Start with** `@CLAUDE.md` + `@INITIAL.md` for foundational context
2. **Use** `@generate-prp.md` to research and plan new features  
3. **Create** feature PRPs using `@prp_base.md` template
4. **Execute** PRPs using `@execute-prp.md` framework
5. **Reference** `@CLAUDE.md` during implementation for technical patterns

This creates a **comprehensive context engineering ecosystem** where each file has a distinct, non-overlapping responsibility while maintaining clear relationships and dependencies.




I've created three complementary Mermaid diagrams showing different perspectives on the file dependencies:

## **Diagram 1: Complete Ecosystem**
Shows the full context including external documentation and codebase analysis feeding into the system, with clear foundation → planning → execution → output flow.



```mermaid
graph TD
    A[["@CLAUDE.md<br/>Technical Patterns<br/>& Development Guide"]] 
    B[["@INITIAL.md<br/>Project Overview<br/>& Business Context"]]
    C[["@generate-prp.md<br/>PRP Creation<br/>Process"]]
    D[["@prp_base.md<br/>Feature Planning<br/>Template"]]
    E[["@execute-prp.md<br/>PRP Execution<br/>Framework"]]
    F[["Individual PRPs<br/>(Feature Plans)"]]
    G[["Working Features<br/>(Implemented Code)"]]
    H[["External Documentation<br/>(Databricks, Domain Specs)"]]
    I[["Codebase Analysis<br/>(Existing Patterns)"]]

    %% Foundational Dependencies
    H -->|"Consolidates & References"| A
    A -->|"Technical Context"| C
    B -->|"Business Context"| C
    I -->|"Existing Patterns"| C

    %% Planning Flow
    C -->|"Research & Generate"| F
    D -->|"Template Structure"| F
    A -->|"Implementation Patterns"| D
    B -->|"Domain Context"| D

    %% Execution Flow
    F -->|"Feature Specifications"| E
    E -->|"Executes Plans"| G
    A -->|"Technical Reference"| E

    %% Feedback Loops
    G -.->|"Updates Patterns"| A
    G -.->|"Refines Process"| E
    F -.->|"Improves Template"| D

    %% Usage Dependencies
    A -->|"Development Reference"| G
    B -->|"Requirements Context"| G

    %% Styling
    classDef foundation fill:#e1f5fe,stroke:#01579b,stroke-width:3px
    classDef process fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef template fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef execution fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef output fill:#fff8e1,stroke:#f57f17,stroke-width:2px
    classDef external fill:#fafafa,stroke:#616161,stroke-width:1px

    class A,B foundation
    class C process
    class D template
    class E execution
    class F,G output
    class H,I external
```




## **Diagram 2: Layered Architecture** 
Organizes the 5 files into logical layers:
- **Foundation Layer**: @CLAUDE.md & @INITIAL.md (stable knowledge bases)
- **Planning Layer**: @generate-prp.md & @prp_base.md (feature planning)
- **Execution Layer**: @execute-prp.md (implementation)



```mermaid
graph TD
    subgraph "Foundation Layer"
        A[["@CLAUDE.md<br/>📋 Technical Patterns<br/>Development Guide"]]
        B[["@INITIAL.md<br/>🏢 Business Context<br/>Project Overview"]]
    end

    subgraph "Planning Layer"
        C[["@generate-prp.md<br/>🔍 PRP Creation<br/>Research Process"]]
        D[["@prp_base.md<br/>📝 Planning Template<br/>Feature Framework"]]
    end

    subgraph "Execution Layer"
        E[["@execute-prp.md<br/>⚡ Execution Framework<br/>Implementation Engine"]]
    end

    subgraph "Outputs"
        F[["Individual PRPs<br/>📄 Feature Plans"]]
        G[["Working Features<br/>✅ Implemented Code"]]
    end

    %% Primary Dependencies
    A -->|"Technical Context<br/>Implementation Patterns"| C
    B -->|"Business Context<br/>Domain Requirements"| C
    A -->|"Code Examples<br/>Development Patterns"| D
    B -->|"Domain Schemas<br/>Business Rules"| D

    %% Planning Flow
    C -->|"Research &<br/>Generate Plans"| F
    D -->|"Template<br/>Structure"| F

    %% Execution Flow
    F -->|"Feature<br/>Specifications"| E
    E -->|"Deploy &<br/>Validate"| G
    A -->|"Technical<br/>Reference"| E

    %% Feedback Loops
    G -.->|"Pattern Updates"| A
    G -.->|"Process Refinement"| E
    F -.->|"Template Improvement"| D

    %% Direct Usage
    A -->|"Development<br/>Reference"| G
    B -->|"Context<br/>Validation"| G

    %% Styling
    classDef foundation fill:#e3f2fd,stroke:#1976d2,stroke-width:3px,color:#000
    classDef planning fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000
    classDef execution fill:#e8f5e8,stroke:#388e3c,stroke-width:3px,color:#000
    classDef output fill:#fff8e1,stroke:#f57c00,stroke-width:2px,color:#000

    class A,B foundation
    class C,D planning
    class E execution
    class F,G output
```


## **Diagram 3: Information Flow**
Focuses on the types of dependencies:
- **Information/Context** (dotted lines): Knowledge transfer
- **Process/Creates** (solid lines): Active creation/execution
- **Feedback/Updates** (curved dotted): Improvement loops


```mermaid
graph TD
    subgraph "Foundation Layer"
        A[["@CLAUDE.md<br/>📋 Technical Patterns<br/>Development Guide"]]
        B[["@INITIAL.md<br/>🏢 Business Context<br/>Project Overview"]]
    end

    subgraph "Planning Layer"
        C[["@generate-prp.md<br/>🔍 PRP Creation<br/>Research Process"]]
        D[["@prp_base.md<br/>📝 Planning Template<br/>Feature Framework"]]
    end

    subgraph "Execution Layer"
        E[["@execute-prp.md<br/>⚡ Execution Framework<br/>Implementation Engine"]]
    end

    subgraph "Outputs"
        F[["Individual PRPs<br/>📄 Feature Plans"]]
        G[["Working Features<br/>✅ Implemented Code"]]
    end

    %% Primary Dependencies
    A -->|"Technical Context<br/>Implementation Patterns"| C
    B -->|"Business Context<br/>Domain Requirements"| C
    A -->|"Code Examples<br/>Development Patterns"| D
    B -->|"Domain Schemas<br/>Business Rules"| D

    %% Planning Flow
    C -->|"Research &<br/>Generate Plans"| F
    D -->|"Template<br/>Structure"| F

    %% Execution Flow
    F -->|"Feature<br/>Specifications"| E
    E -->|"Deploy &<br/>Validate"| G
    A -->|"Technical<br/>Reference"| E

    %% Feedback Loops
    G -.->|"Pattern Updates"| A
    G -.->|"Process Refinement"| E
    F -.->|"Template Improvement"| D

    %% Direct Usage
    A -->|"Development<br/>Reference"| G
    B -->|"Context<br/>Validation"| G

    %% Styling
    classDef foundation fill:#e3f2fd,stroke:#1976d2,stroke-width:3px,color:#000
    classDef planning fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000
    classDef execution fill:#e8f5e8,stroke:#388e3c,stroke-width:3px,color:#000
    classDef output fill:#fff8e1,stroke:#f57c00,stroke-width:2px,color:#000

    class A,B foundation
    class C,D planning
    class E execution
    class F,G output
```



## **Key Dependency Insights:**

### **Primary Dependencies:**
- `@CLAUDE.md` + `@INITIAL.md` → Foundation for all other files
- `@generate-prp.md` → Creates PRPs using foundation knowledge
- `@prp_base.md` → Template structure for feature planning
- `@execute-prp.md` → Executes completed PRPs

### **Critical Relationships:**
1. **@CLAUDE.md is the technical spine** - referenced by planning, execution, and implementation
2. **@INITIAL.md provides business context** - validates requirements and domain alignment  
3. **@prp_base.md acts as the bridge** - connects abstract planning to concrete implementation
4. **Feedback loops exist** - implemented features improve patterns and processes

### **Dependency Characteristics:**
- **Foundational files** (@CLAUDE.md, @INITIAL.md) have **outgoing dependencies only**
- **Process files** (@generate-prp.md, @execute-prp.md) have **both incoming and outgoing**
- **Template file** (@prp_base.md) is **primarily consumed, rarely updated**
- **Strong separation of concerns** - each file has distinct responsibilities with minimal overlap

This dependency structure ensures **modular, maintainable context engineering** where changes propagate predictably through the system.



