# PRP Generate and Execute Workflow Diagram

```mermaid
flowchart LR

    subgraph inputs ["Required Inputs"]
      A["CLAUDE.md<br/>Project-wide rules<br/>HOW to Implement<br/>Tech Domain Specific<br/>ie. Apps, LDP etc"] --> B["ldp_health_medallion.md<br/>LDP Feature"]
    end
    
    B1["ml_model_pipeline.md<br/>ML Feature"]
    B2["web_application.md<br/>Apps Feature"]
    
    A --> B1
    A --> B2
    B --> C(["Generate PRP"])
    C --> D["/generate-prp<br/>ldp_health_medallion.md"]
    
    D --> E["Product<br/>Requirements<br/>Prompt Doc<br/>Feature Planning<br/>PRP"]
    
    E --> F(["Implement Feature"])
    
    F --> I["/execute-prp PRPs/feature-prp.md"]

    I --> J["Fully Implemented Feature"]
    I --> K["Fully Implemented Feature"]
    
    subgraph additional ["Fully Implemented Feature"]
        J["App Feature"]
        K["ML Feature"]
    end

    
    style A fill:#ffcccc
    style B fill:#ffcccc
    style B1 fill:#ffcccc,stroke-dasharray: 5 5
    style B2 fill:#ffcccc,stroke-dasharray: 5 5
    style E fill:#ccffcc
    style additional fill:#f0f0f0,stroke-dasharray: 5 5
```

## Workflow Description

This diagram illustrates the planning and implementation workflow for features in the Databricks Context Engineering project:

1. **Pre-existing Planning & Context Inputs** (pink boxes):
   - CLAUDE.md provides project-wide implementation rules
   - Domain-specific documentation guides feature development

2. **Feature Planning Process**:
   - Generate Product Requirements Prompt (PRP) from base documentation
   - Create detailed implementation plans with validation steps
   - Store PRPs for execution

3. **Implementation Phase (Single Pass, Fully Architected Feature)**:
   - Execute features based on generated PRPs

4. **Additional Features**:
   - Can be added as separate base files with their own CLAUDE.md guides
   - Follow the same generate-prp → implement cycle
