# C4 Model Architectural Diagrams

## Level 1: System Context Diagram

```mermaid
graph TB
    subgraph "External Users"
        DA[Data Analysts]
        BE[Business Executives]
        DS[Data Scientists]
        APP[Applications]
    end

    subgraph "External Systems"
        ORA[Oracle Database]
        SQL[SQL Server]
        DV[Microsoft Dataverse]
        EXT[External APIs]
    end

    subgraph "Enterprise Data Platform"
        EDP[Data Platform Core<br/>Databricks + Unity Catalog]
    end

    subgraph "Consumption Layer"
        PBI[Power BI]
        DBSQL[Databricks SQL]
        CAPP[Custom Applications]
        API[REST APIs]
    end

    %% Data Sources to Platform
    ORA -->|JDBC| EDP
    SQL -->|JDBC| EDP
    DV -->|REST API| EDP
    EXT -->|HTTP/APIs| EDP

    %% Platform to Consumption
    EDP -->|Delta Tables| PBI
    EDP -->|SQL Endpoint| DBSQL
    EDP -->|APIs| CAPP
    EDP -->|REST| API

    %% Users to Consumption
    DA --> DBSQL
    DA --> PBI
    BE --> PBI
    DS --> DBSQL
    DS --> EDP
    APP --> API
    APP --> CAPP

    style EDP fill:#e1f5fe
    style ORA fill:#fff3e0
    style SQL fill:#fff3e0
    style DV fill:#fff3e0
```

## Level 2: Container Diagram

```mermaid
graph TB
    subgraph "Data Sources"
        ORA[Oracle DB]
        SQL[SQL Server]
        DV[Dataverse]
    end

    subgraph "Databricks Platform"
        subgraph "Compute Layer"
            SC[Spark Clusters]
            DLT[Delta Live Tables]
            JOBS[Databricks Jobs]
        end

        subgraph "Storage Layer"
            DL[Delta Lake Storage]
            UC[Unity Catalog]
            ML[MLflow Registry]
        end

        subgraph "Processing Layer"
            PROC[Data Processing<br/>Spark Applications]
            PIPE[ETL Pipeline<br/>Components]
            QUAL[Data Quality<br/>Framework]
        end

        subgraph "Analytics Layer"
            SQLEP[SQL Endpoint]
            NB[Notebooks]
            DASH[Dashboards]
        end
    end

    subgraph "Development & Operations"
        GIT[Git Repository]
        CICD[CI/CD Pipeline]
        LOCAL[Local Development<br/>Docker Environment]
    end

    subgraph "Governance & Security"
        AAD[Azure AD/IAM]
        RBAC[Role-Based Access]
        AUDIT[Audit Logging]
        SECRETS[Secret Management]
    end

    %% Data flow
    ORA -->|Connectors| DLT
    SQL -->|Connectors| DLT
    DV -->|REST API| JOBS

    DLT --> DL
    JOBS --> DL
    PROC --> DL

    DL --> UC
    UC --> SQLEP
    UC --> NB

    %% Development flow
    GIT --> CICD
    CICD --> PROC
    CICD --> PIPE
    LOCAL --> GIT

    %% Security
    AAD --> RBAC
    RBAC --> UC
    RBAC --> SQLEP
    SECRETS --> PROC

    style DL fill:#4fc3f7
    style UC fill:#81c784
    style DLT fill:#ffb74d
```

## Level 3: Component Diagram - Data Processing Layer

```mermaid
graph TB
    subgraph "Source Adapters"
        OA[Oracle Adapter]
        SA[SQL Server Adapter]
        DA[Dataverse Adapter]
    end

    subgraph "Data Processing Components"
        subgraph "Ingestion Layer"
            IR[Ingestion Router]
            CDC[Change Data Capture]
            BATCH[Batch Processor]
        end

        subgraph "Transformation Layer"
            TR[Transformation Router]
            CLEAN[Data Cleaner]
            ENRICH[Data Enricher]
            VALID[Data Validator]
        end

        subgraph "Quality Framework"
            QR[Quality Rules Engine]
            QM[Quality Monitor]
            AL[Alerting System]
        end
    end

    subgraph "Storage Abstraction"
        WRITER[Delta Writer]
        READER[Delta Reader]
        META[Metadata Manager]
    end

    subgraph "Pipeline Orchestration"
        COORD[Pipeline Coordinator]
        SCHED[Scheduler]
        RETRY[Retry Handler]
        STATE[State Manager]
    end

    %% Component interactions
    OA --> IR
    SA --> IR
    DA --> IR

    IR --> CDC
    IR --> BATCH

    CDC --> TR
    BATCH --> TR

    TR --> CLEAN
    TR --> ENRICH
    CLEAN --> VALID
    ENRICH --> VALID

    VALID --> QR
    QR --> QM
    QM --> AL

    VALID --> WRITER
    WRITER --> META

    COORD --> SCHED
    COORD --> STATE
    SCHED --> RETRY

    style IR fill:#ffecb3
    style TR fill:#c8e6c9
    style QR fill:#f8bbd9
    style WRITER fill:#b3e5fc
```

## Level 4: Code Structure - Component Implementation

```mermaid
classDiagram
    class SourceAdapter {
        <<interface>>
        +connect() Connection
        +read_table(table_name) DataFrame
        +get_schema(table_name) Schema
        +test_connection() boolean
    }

    class OracleAdapter {
        -config: OracleConfig
        -connection_pool: ConnectionPool
        +connect() Connection
        +read_table(table_name) DataFrame
        +read_incremental(table_name, watermark) DataFrame
    }

    class DataProcessor {
        <<interface>>
        +process(df: DataFrame) DataFrame
        +validate(df: DataFrame) ValidationResult
    }

    class CleaningProcessor {
        -rules: List~CleaningRule~
        +process(df: DataFrame) DataFrame
        +add_rule(rule: CleaningRule) void
    }

    class EnrichmentProcessor {
        -enrichment_sources: Dict
        +process(df: DataFrame) DataFrame
        +add_enrichment(source: EnrichmentSource) void
    }

    class QualityFramework {
        -expectations: List~Expectation~
        -monitors: List~QualityMonitor~
        +validate_data(df: DataFrame) QualityReport
        +add_expectation(expectation: Expectation) void
    }

    class PipelineOrchestrator {
        -processors: List~DataProcessor~
        -config: PipelineConfig
        +execute_pipeline(source_data) Result
        +add_processor(processor: DataProcessor) void
    }

    class UnityGateway {
        -catalog_client: CatalogClient
        +register_table(table_info: TableInfo) void
        +get_table_schema(table_name) Schema
        +apply_governance_policy(policy: GovernancePolicy) void
    }

    SourceAdapter <|-- OracleAdapter
    DataProcessor <|-- CleaningProcessor
    DataProcessor <|-- EnrichmentProcessor
    
    PipelineOrchestrator --> DataProcessor
    PipelineOrchestrator --> SourceAdapter
    PipelineOrchestrator --> QualityFramework
    PipelineOrchestrator --> UnityGateway

    OracleAdapter --> UnityGateway
    QualityFramework --> UnityGateway
```

## Data Flow Diagram

```mermaid
sequenceDiagram
    participant S as Source System
    participant A as Source Adapter
    participant I as Ingestion Engine
    participant P as Processing Pipeline
    participant Q as Quality Framework
    participant D as Delta Lake
    participant U as Unity Catalog
    participant C as Consumer

    S->>A: Raw Data
    A->>I: Structured Data
    I->>P: Validated Input
    
    P->>P: Transform Data
    P->>Q: Quality Check
    
    alt Quality Check Passes
        Q->>D: Write to Delta
        D->>U: Register Metadata
        U->>C: Data Available
    else Quality Check Fails
        Q->>I: Retry/Alert
    end

    Note over P: Composition-based<br/>processing chain
    Note over D,U: Native Databricks<br/>integration
```

## Deployment Architecture

```mermaid
graph TB
    subgraph "Development Environment"
        DEV_LOCAL[Local Docker<br/>Development]
        DEV_GIT[Git Repository]
        DEV_TEST[Unit Tests]
    end

    subgraph "CI/CD Pipeline"
        BUILD[Build & Test]
        DEPLOY[Databricks<br/>Asset Bundle]
        VALIDATE[Integration<br/>Tests]
    end

    subgraph "Staging Environment"
        STAGE_WS[Databricks<br/>Staging Workspace]
        STAGE_UC[Unity Catalog<br/>Staging]
        STAGE_DATA[Test Data]
    end

    subgraph "Production Environment"
        PROD_WS[Databricks<br/>Production Workspace]
        PROD_UC[Unity Catalog<br/>Production]
        PROD_DATA[Production Data]
        PROD_MON[Monitoring &<br/>Alerting]
    end

    DEV_LOCAL --> DEV_GIT
    DEV_GIT --> BUILD
    BUILD --> DEPLOY
    DEPLOY --> STAGE_WS
    STAGE_WS --> VALIDATE
    VALIDATE --> PROD_WS

    STAGE_WS --> STAGE_UC
    STAGE_WS --> STAGE_DATA

    PROD_WS --> PROD_UC
    PROD_WS --> PROD_DATA
    PROD_WS --> PROD_MON

    style PROD_WS fill:#c8e6c9
    style STAGE_WS fill:#fff3e0
    style DEV_LOCAL fill:#e1f5fe
```

## Security Architecture

```mermaid
graph TB
    subgraph "Authentication Layer"
        AAD[Azure AD/AWS IAM]
        SSO[Single Sign-On]
        MFA[Multi-Factor Auth]
    end

    subgraph "Authorization Layer"
        RBAC[Role-Based Access Control]
        ABAC[Attribute-Based Access]
        POL[Data Policies]
    end

    subgraph "Data Layer Security"
        ENC[Encryption at Rest]
        TLS[TLS in Transit]
        MASK[Data Masking]
        RLS[Row Level Security]
    end

    subgraph "Monitoring & Audit"
        LOG[Audit Logging]
        MON[Security Monitoring]
        ALERT[Threat Detection]
    end

    AAD --> RBAC
    SSO --> RBAC
    MFA --> RBAC

    RBAC --> POL
    ABAC --> POL
    POL --> RLS
    POL --> MASK

    RLS --> LOG
    MASK --> LOG
    ENC --> MON
    TLS --> MON

    LOG --> ALERT
    MON --> ALERT

    style AAD fill:#ffcdd2
    style RBAC fill:#f8bbd9
    style ENC fill:#c8e6c9
    style LOG fill:#fff3e0
```

## Performance Architecture

```mermaid
graph TB
    subgraph "Compute Optimization"
        AUTO[Auto-scaling Clusters]
        POOL[Instance Pools]
        SPOT[Spot Instances]
        CACHE[Delta Cache]
    end

    subgraph "Storage Optimization"
        PART[Data Partitioning]
        COMP[Compression]
        OPT[Table Optimization]
        LIQUID[Liquid Clustering]
    end

    subgraph "Query Optimization"
        INDEX[Z-Order Indexing]
        PRED[Predicate Pushdown]
        BLOOM[Bloom Filters]
        STATS[Statistics]
    end

    subgraph "Monitoring"
        PERF[Performance Metrics]
        COST[Cost Tracking]
        ALERT_PERF[Performance Alerts]
    end

    AUTO --> PERF
    POOL --> COST
    SPOT --> COST

    PART --> OPT
    COMP --> OPT
    OPT --> INDEX

    INDEX --> STATS
    PRED --> STATS
    BLOOM --> STATS

    PERF --> ALERT_PERF
    COST --> ALERT_PERF
    STATS --> PERF

    style AUTO fill:#b3e5fc
    style OPT fill:#c8e6c9
    style INDEX fill:#ffecb3
    style PERF fill:#f8bbd9
```

These diagrams provide a comprehensive view of the enterprise data platform architecture at multiple levels of detail, following C4 model conventions for clear communication with stakeholders.