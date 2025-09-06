# Enterprise Data Platform Architecture

## Executive Summary

This architecture leverages Databricks native capabilities while maintaining development flexibility and transparency. The design prioritizes composition over inheritance, minimal abstraction layers, and clear reasoning paths.

## Core Architectural Principles

1. **Databricks-Native First**: Leverage Unity Catalog, Delta Lake, Databricks SQL, and Workflows
2. **Local Development Compatibility**: Enable local testing and development without cloud dependencies
3. **Minimal Abstraction**: Prefer direct service usage over heavy wrapper frameworks
4. **Composition over Inheritance**: Build flexible, reusable components through composition
5. **Transparent Reasoning**: Clear data lineage and decision audit trails

## High-Level Architecture (C4 Level 1 - System Context)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Enterprise Data Platform                      │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│  │   Oracle    │    │ SQL Server  │    │ Dataverse   │        │
│  │  Sources    │    │  Sources    │    │  Sources    │        │
│  └─────────────┘    └─────────────┘    └─────────────┘        │
│         │                  │                  │                │
│         └──────────────────┼──────────────────┘                │
│                           │                                    │
│  ┌─────────────────────────┼─────────────────────────────────┐ │
│  │        Data Platform Core (Databricks)                    │ │
│  │                        │                                  │ │
│  │  ┌─────────────┐    ┌──┴──────────┐    ┌─────────────┐   │ │
│  │  │   Unity     │    │    Delta    │    │ Databricks  │   │ │
│  │  │  Catalog    │    │    Lake     │    │ Workflows   │   │ │
│  │  └─────────────┘    └─────────────┘    └─────────────┘   │ │
│  └─────────────────────────────────────────────────────────┘ │
│                           │                                    │
│  ┌─────────────────────────┼─────────────────────────────────┐ │
│  │           Analytics & Consumption Layer                   │ │
│  │  ┌─────────────┐    ┌──┴──────────┐    ┌─────────────┐   │ │
│  │  │   Power     │    │ Databricks  │    │   Custom    │   │ │
│  │  │     BI      │    │    SQL      │    │    Apps     │   │ │
│  │  └─────────────┘    └─────────────┘    └─────────────┘   │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Component Architecture (C4 Level 2 - Container Diagram)

### Data Ingestion Layer
- **Source Connectors**: Databricks-native connectors for Oracle, SQL Server, Dataverse
- **Change Data Capture**: Delta Live Tables for real-time data streaming
- **Batch Processing**: Databricks Jobs for scheduled data loads
- **Data Quality**: Expectations framework built on Delta Live Tables

### Storage and Catalog Layer
- **Unity Catalog**: Centralized governance and metadata management
- **Delta Lake**: ACID transactions, time travel, and schema evolution
- **Data Lineage**: Built-in Unity Catalog lineage tracking
- **Access Control**: Unity Catalog RBAC and attribute-based access control

### Processing Layer
- **Spark Clusters**: Auto-scaling compute for data processing
- **Delta Live Tables**: Declarative ETL pipelines
- **Machine Learning**: MLflow integration with Unity Catalog
- **SQL Analytics**: Databricks SQL for interactive analytics

### Development and Deployment
- **Local Development**: Docker-based local environments
- **CI/CD Integration**: Databricks Asset Bundles for deployment automation
- **Version Control**: Git integration with Databricks Repos
- **Testing Framework**: Local unit tests and integration test patterns

## Data Source Integration Pattern

### Oracle Integration
```python
class OracleSourceConfig:
    def __init__(self, connection_params: Dict[str, str]):
        self.host = connection_params['host']
        self.port = connection_params['port']
        self.service_name = connection_params['service_name']
        self.username = connection_params['username']
        self.password_secret = connection_params['password_secret']
    
    def to_jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.service_name}"

class DatabricksOracleReader:
    def __init__(self, config: OracleSourceConfig):
        self.config = config
    
    def read_table(self, table_name: str, spark_session) -> DataFrame:
        return spark_session.read \
            .format("jdbc") \
            .option("url", self.config.to_jdbc_url()) \
            .option("dbtable", table_name) \
            .option("user", self.config.username) \
            .option("password", dbutils.secrets.get("oracle", self.config.password_secret)) \
            .load()
```

### SQL Server Integration
```python
class SQLServerSourceConfig:
    def __init__(self, connection_params: Dict[str, str]):
        self.server = connection_params['server']
        self.database = connection_params['database']
        self.username = connection_params['username']
        self.password_secret = connection_params['password_secret']
    
    def to_jdbc_url(self) -> str:
        return f"jdbc:sqlserver://{self.server};databaseName={self.database}"

class DatabricksSQLServerReader:
    def __init__(self, config: SQLServerSourceConfig):
        self.config = config
    
    def read_table(self, table_name: str, spark_session) -> DataFrame:
        return spark_session.read \
            .format("jdbc") \
            .option("url", self.config.to_jdbc_url()) \
            .option("dbtable", table_name) \
            .option("user", self.config.username) \
            .option("password", dbutils.secrets.get("sqlserver", self.config.password_secret)) \
            .load()
```

### Dataverse Integration
```python
class DataverseSourceConfig:
    def __init__(self, connection_params: Dict[str, str]):
        self.environment_url = connection_params['environment_url']
        self.client_id = connection_params['client_id']
        self.client_secret = connection_params['client_secret']
        self.tenant_id = connection_params['tenant_id']

class DatabricksDataverseReader:
    def __init__(self, config: DataverseSourceConfig):
        self.config = config
    
    def read_entity(self, entity_name: str, spark_session) -> DataFrame:
        # Use Dataverse REST API or OData endpoint
        # Implementation would use Databricks' HTTP connector
        pass
```

## Local Development Pattern

### Docker-Based Local Environment
```yaml
# docker-compose.yml
version: '3.8'
services:
  spark-local:
    image: apache/spark:3.4.0
    environment:
      - SPARK_MODE=standalone
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ./src:/opt/spark/work-dir/src
      - ./data:/opt/spark/work-dir/data
  
  postgres-mock:
    image: postgres:13
    environment:
      POSTGRES_DB: mockdata
      POSTGRES_USER: testuser
      POSTGRES_PASSWORD: testpass
    ports:
      - "5432:5432"
    volumes:
      - ./mock-data:/docker-entrypoint-initdb.d
```

### Local Testing Framework
```python
class LocalTestEnvironment:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("LocalTest") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
    
    def create_mock_source_data(self, source_type: str, table_name: str, data: List[Dict]):
        df = self.spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable(f"mock_{source_type}_{table_name}")
    
    def test_pipeline(self, pipeline_function):
        # Execute pipeline with mock data
        result = pipeline_function(self.spark)
        return result
```

## Composition-Based Component Design

### Pipeline Component Composition
```python
from abc import ABC, abstractmethod
from typing import Protocol

class DataProcessor(Protocol):
    def process(self, df: DataFrame) -> DataFrame:
        ...

class DataValidator(Protocol):
    def validate(self, df: DataFrame) -> bool:
        ...

class DataWriter(Protocol):
    def write(self, df: DataFrame, destination: str) -> None:
        ...

# Concrete implementations
class RowLevelSecurityProcessor:
    def __init__(self, user_context: str):
        self.user_context = user_context
    
    def process(self, df: DataFrame) -> DataFrame:
        return df.filter(f"access_level <= '{self.user_context}'")

class SchemaValidator:
    def __init__(self, expected_schema: StructType):
        self.expected_schema = expected_schema
    
    def validate(self, df: DataFrame) -> bool:
        return df.schema == self.expected_schema

class DeltaWriter:
    def __init__(self, table_path: str):
        self.table_path = table_path
    
    def write(self, df: DataFrame, destination: str) -> None:
        df.write.format("delta").mode("overwrite").save(f"{self.table_path}/{destination}")

# Pipeline composition
class DataPipeline:
    def __init__(self, 
                 processor: DataProcessor,
                 validator: DataValidator,
                 writer: DataWriter):
        self.processor = processor
        self.validator = validator
        self.writer = writer
    
    def execute(self, source_df: DataFrame, destination: str):
        processed_df = self.processor.process(source_df)
        
        if not self.validator.validate(processed_df):
            raise ValueError("Data validation failed")
        
        self.writer.write(processed_df, destination)
```

## Unity Catalog Integration

### Catalog Structure
```
enterprise_catalog/
├── raw/                    # Bronze layer - raw ingested data
│   ├── oracle_crm/
│   ├── sqlserver_erp/
│   └── dataverse_marketing/
├── refined/               # Silver layer - cleaned and enriched
│   ├── customer_360/
│   ├── financial_reporting/
│   └── marketing_analytics/
└── gold/                 # Gold layer - business-ready aggregations
    ├── executive_dashboards/
    ├── regulatory_reports/
    └── ml_features/
```

### Governance Implementation
```python
class UnityGoveranceManager:
    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url
        self.token = token
    
    def create_schema_with_governance(self, catalog: str, schema: str, 
                                    data_classification: str,
                                    retention_policy: str):
        sql = f"""
        CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
        COMMENT 'Data classification: {data_classification}, Retention: {retention_policy}'
        """
        spark.sql(sql)
    
    def apply_row_level_security(self, table: str, policy_name: str, condition: str):
        sql = f"""
        CREATE OR REPLACE ROW ACCESS POLICY {policy_name}
        ON {table}
        GRANT TO ('data_analysts')
        FILTER USING ({condition})
        """
        spark.sql(sql)
```

## Monitoring and Observability

### Data Quality Monitoring
```python
class DataQualityMonitor:
    def __init__(self, unity_catalog: str):
        self.unity_catalog = unity_catalog
    
    def create_quality_checks(self, table_name: str, checks: List[str]):
        for check in checks:
            sql = f"""
            CREATE OR REPLACE EXPECTATION {check}_check
            EXPECT ({check})
            ON VIOLATION DROP ROW
            """
            spark.sql(sql)
    
    def monitor_data_freshness(self, table_name: str, max_delay_hours: int):
        sql = f"""
        SELECT 
            CASE 
                WHEN DATEDIFF(hour, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > {max_delay_hours}
                THEN 'STALE'
                ELSE 'FRESH'
            END as freshness_status
        FROM {table_name}
        """
        return spark.sql(sql)
```

### Lineage Tracking
```python
class LineageTracker:
    def __init__(self):
        self.lineage_table = "system.information_schema.table_lineage"
    
    def get_downstream_dependencies(self, table_name: str):
        sql = f"""
        WITH RECURSIVE lineage AS (
            SELECT source_table_full_name, target_table_full_name
            FROM {self.lineage_table}
            WHERE source_table_full_name = '{table_name}'
            
            UNION ALL
            
            SELECT t.source_table_full_name, t.target_table_full_name
            FROM {self.lineage_table} t
            JOIN lineage l ON t.source_table_full_name = l.target_table_full_name
        )
        SELECT DISTINCT target_table_full_name FROM lineage
        """
        return spark.sql(sql)
```

## Deployment Architecture

### Environment Strategy
- **Development**: Local Docker + Databricks Community Edition
- **Staging**: Databricks workspace with reduced compute
- **Production**: Databricks workspace with auto-scaling clusters

### CI/CD Pipeline with Databricks Asset Bundles
```yaml
# databricks.yml
bundle:
  name: enterprise-data-platform
  
workspace:
  host: ${DATABRICKS_HOST}
  
environments:
  development:
    default: true
    workspace:
      host: https://community.cloud.databricks.com
    
  staging:
    workspace:
      host: ${STAGING_WORKSPACE_URL}
    
  production:
    workspace:
      host: ${PROD_WORKSPACE_URL}

resources:
  jobs:
    oracle_ingestion:
      name: "Oracle Data Ingestion"
      job_clusters:
        - job_cluster_key: "oracle_cluster"
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
      
      tasks:
        - task_key: "ingest_oracle_data"
          job_cluster_key: "oracle_cluster"
          notebook_task:
            notebook_path: "./notebooks/oracle_ingestion"
```

## Security Architecture

### Authentication and Authorization
- **Workspace Authentication**: Azure AD/AWS IAM integration
- **Data Access**: Unity Catalog RBAC with fine-grained permissions
- **Secrets Management**: Databricks Secrets API with Azure Key Vault/AWS Secrets Manager
- **Network Security**: Private endpoints and VPC/VNet integration

### Data Encryption
- **At Rest**: Delta Lake encryption with customer-managed keys
- **In Transit**: TLS 1.2+ for all data transfers
- **Processing**: Encrypted Spark shuffle and caching

## Cost Optimization Strategy

### Compute Optimization
- **Auto-scaling clusters**: Dynamic resource allocation based on workload
- **Spot instances**: Cost-effective compute for non-critical workloads
- **Pool management**: Shared compute pools to reduce startup times
- **Scheduled scaling**: Predictive scaling based on usage patterns

### Storage Optimization
- **Delta Lake optimization**: Regular OPTIMIZE and VACUUM operations
- **Data lifecycle**: Automated archival to cheaper storage tiers
- **Compression**: Optimal file formats and compression algorithms

## Migration Strategy

### Phase 1: Foundation (Months 1-2)
1. Set up Databricks workspace and Unity Catalog
2. Establish basic connectivity to source systems
3. Implement core data ingestion patterns
4. Set up local development environment

### Phase 2: Core Implementation (Months 3-4)
1. Migrate critical data pipelines
2. Implement data quality monitoring
3. Set up CI/CD processes
4. Establish security and governance controls

### Phase 3: Advanced Features (Months 5-6)
1. Implement advanced analytics capabilities
2. Deploy machine learning workflows
3. Optimize performance and costs
4. Full production rollout

## Risk Mitigation

### Technical Risks
- **Vendor Lock-in**: Use standard Spark APIs where possible
- **Performance**: Regular performance testing and optimization
- **Data Quality**: Comprehensive validation and monitoring
- **Security**: Multi-layered security approach

### Operational Risks
- **Skills Gap**: Training programs and documentation
- **Change Management**: Gradual migration with rollback plans
- **Cost Control**: Regular cost monitoring and optimization

## Success Metrics

### Technical Metrics
- **Data Freshness**: < 1 hour for critical datasets
- **Pipeline Reliability**: > 99.5% success rate
- **Query Performance**: < 10 seconds for standard reports
- **Cost per TB**: Track and optimize monthly

### Business Metrics
- **Time to Insight**: Reduce from days to hours
- **Data Governance**: 100% catalog coverage
- **Developer Productivity**: Reduce development time by 50%
- **Compliance**: 100% audit trail coverage

This architecture provides a solid foundation for enterprise data platform that leverages Databricks native capabilities while maintaining flexibility and transparency in implementation.