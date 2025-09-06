# Code Patterns: Databricks Development Best Practices

## 🏗️ Architecture Patterns

### 1. Configuration Management Pattern

```python
# config/databricks_config.py
import os
from dataclasses import dataclass
from typing import Optional
from databricks.sdk import WorkspaceClient

@dataclass
class DatabricksConfig:
    """Environment-specific Databricks configuration"""
    workspace_url: str
    catalog: str
    schema: str
    cluster_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    
    @classmethod
    def from_environment(cls, env: str = "dev") -> "DatabricksConfig":
        """Load configuration from environment variables"""
        return cls(
            workspace_url=os.getenv(f"DATABRICKS_{env.upper()}_URL"),
            catalog=os.getenv(f"DATABRICKS_{env.upper()}_CATALOG"),
            schema=os.getenv(f"DATABRICKS_{env.upper()}_SCHEMA"),
            cluster_id=os.getenv(f"DATABRICKS_{env.upper()}_CLUSTER_ID"),
            warehouse_id=os.getenv(f"DATABRICKS_{env.upper()}_WAREHOUSE_ID")
        )
    
    def get_client(self) -> WorkspaceClient:
        """Get authenticated Databricks client"""
        return WorkspaceClient(host=self.workspace_url)

# Usage Example
config = DatabricksConfig.from_environment("prod")
client = config.get_client()
```

### 2. Data Source Adapter Pattern

```python
# adapters/base_adapter.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd

class DataSourceAdapter(ABC):
    """Base class for data source adapters"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._connection = None
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to data source"""
        pass
    
    @abstractmethod
    def extract_data(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def get_schema(self, table_name: str) -> Dict[str, str]:
        """Get table schema information"""
        pass
    
    def close(self) -> None:
        """Close connection"""
        if self._connection:
            self._connection.close()

# adapters/oracle_adapter.py
import cx_Oracle
import pandas as pd
from .base_adapter import DataSourceAdapter

class OracleAdapter(DataSourceAdapter):
    """Oracle database adapter"""
    
    def connect(self) -> None:
        """Establish Oracle connection"""
        self._connection = cx_Oracle.connect(self.connection_string)
    
    def extract_data(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Extract data from Oracle"""
        if not self._connection:
            self.connect()
        
        return pd.read_sql(query, self._connection, params=params)
    
    def get_schema(self, table_name: str) -> Dict[str, str]:
        """Get Oracle table schema"""
        schema_query = """
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM USER_TAB_COLUMNS 
        WHERE TABLE_NAME = :table_name
        """
        schema_df = self.extract_data(schema_query, {"table_name": table_name.upper()})
        return dict(zip(schema_df["COLUMN_NAME"], schema_df["DATA_TYPE"]))
```

### 3. Delta Table Operations Pattern

```python
# utils/delta_operations.py
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import structlog

logger = structlog.get_logger()

class DeltaTableManager:
    """Utility class for Delta table operations"""
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def create_table_if_not_exists(
        self, 
        table_name: str, 
        df: DataFrame, 
        partition_by: Optional[List[str]] = None
    ) -> None:
        """Create Delta table if it doesn't exist"""
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        try:
            # Check if table exists
            self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
            logger.info("Table already exists", table=full_table_name)
        except Exception:
            # Table doesn't exist, create it
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.saveAsTable(full_table_name)
            logger.info("Created Delta table", table=full_table_name, partitions=partition_by)
    
    def merge_incremental_data(
        self, 
        source_df: DataFrame, 
        target_table: str, 
        merge_keys: List[str],
        update_condition: Optional[str] = None
    ) -> Dict[str, int]:
        """Merge incremental data using Delta MERGE"""
        full_table_name = f"{self.catalog}.{self.schema}.{target_table}"
        temp_view = f"temp_{target_table}_source"
        
        # Create temporary view for source data
        source_df.createOrReplaceTempView(temp_view)
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Build update condition
        if not update_condition:
            update_condition = "source._timestamp > target._timestamp"
        
        merge_sql = f"""
        MERGE INTO {full_table_name} target
        USING {temp_view} source
        ON {merge_condition}
        WHEN MATCHED AND {update_condition} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        result = self.spark.sql(merge_sql)
        
        # Get merge statistics
        merge_stats = self.spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").collect()[0]
        
        logger.info(
            "Completed Delta merge",
            table=full_table_name,
            operation_metrics=merge_stats["operationMetrics"]
        )
        
        return merge_stats["operationMetrics"]
    
    def optimize_table(self, table_name: str, z_order_columns: Optional[List[str]] = None) -> None:
        """Optimize Delta table with optional Z-ordering"""
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        # Run OPTIMIZE
        optimize_sql = f"OPTIMIZE {full_table_name}"
        if z_order_columns:
            optimize_sql += f" ZORDER BY ({', '.join(z_order_columns)})"
        
        self.spark.sql(optimize_sql)
        
        # Run VACUUM (retain 7 days)
        self.spark.sql(f"VACUUM {full_table_name} RETAIN 168 HOURS")
        
        logger.info(
            "Optimized Delta table",
            table=full_table_name,
            z_order_columns=z_order_columns
        )
```

### 4. Error Handling and Retry Pattern

```python
# utils/error_handling.py
import time
import structlog
from functools import wraps
from typing import Type, Tuple, Callable, Any
from databricks.sdk.errors import DatabricksError

logger = structlog.get_logger()

class RetryableError(Exception):
    """Exception that can be retried"""
    pass

class PermanentError(Exception):
    """Exception that should not be retried"""
    pass

def retry_on_error(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (RetryableError, DatabricksError)
) -> Callable:
    """Decorator to retry function on specific exceptions"""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            "Function failed after max retries",
                            function=func.__name__,
                            attempt=attempt + 1,
                            error=str(e)
                        )
                        raise
                    
                    wait_time = backoff_factor ** attempt
                    logger.warning(
                        "Function failed, retrying",
                        function=func.__name__,
                        attempt=attempt + 1,
                        wait_time=wait_time,
                        error=str(e)
                    )
                    time.sleep(wait_time)
                except Exception as e:
                    logger.error(
                        "Function failed with non-retryable error",
                        function=func.__name__,
                        error=str(e)
                    )
                    raise PermanentError(f"Non-retryable error: {e}") from e
            
            raise last_exception
        
        return wrapper
    return decorator

# Usage Example
@retry_on_error(max_retries=3, backoff_factor=2.0)
def create_delta_table(table_name: str, data_df: DataFrame) -> None:
    """Create Delta table with retry logic"""
    data_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
```

### 5. Data Quality Validation Pattern

```python
# utils/data_quality.py
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from dataclasses import dataclass
import structlog

logger = structlog.get_logger()

@dataclass
class QualityRule:
    """Data quality rule definition"""
    name: str
    description: str
    sql_condition: str
    error_threshold: float = 0.0  # Percentage of records that can fail
    severity: str = "ERROR"  # ERROR, WARNING, INFO

class DataQualityValidator:
    """Data quality validation framework"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def validate_dataframe(
        self, 
        df: DataFrame, 
        rules: List[QualityRule],
        table_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Validate DataFrame against quality rules"""
        
        total_records = df.count()
        validation_results = {
            "table_name": table_name,
            "total_records": total_records,
            "rules_passed": 0,
            "rules_failed": 0,
            "rule_results": []
        }
        
        # Create temporary view for validation
        temp_view = "quality_validation_temp"
        df.createOrReplaceTempView(temp_view)
        
        for rule in rules:
            try:
                # Count records that fail the rule
                failed_count = self.spark.sql(
                    f"SELECT COUNT(*) as count FROM {temp_view} WHERE NOT ({rule.sql_condition})"
                ).collect()[0]["count"]
                
                failure_rate = (failed_count / total_records) * 100 if total_records > 0 else 0
                passed = failure_rate <= rule.error_threshold
                
                rule_result = {
                    "rule_name": rule.name,
                    "description": rule.description,
                    "failed_records": failed_count,
                    "failure_rate": failure_rate,
                    "threshold": rule.error_threshold,
                    "passed": passed,
                    "severity": rule.severity
                }
                
                validation_results["rule_results"].append(rule_result)
                
                if passed:
                    validation_results["rules_passed"] += 1
                    logger.info("Quality rule passed", **rule_result)
                else:
                    validation_results["rules_failed"] += 1
                    if rule.severity == "ERROR":
                        logger.error("Quality rule failed", **rule_result)
                    else:
                        logger.warning("Quality rule failed", **rule_result)
                        
            except Exception as e:
                logger.error(
                    "Error executing quality rule",
                    rule_name=rule.name,
                    error=str(e)
                )
                validation_results["rules_failed"] += 1
        
        return validation_results
    
    def validate_table(
        self, 
        table_name: str, 
        rules: List[QualityRule]
    ) -> Dict[str, Any]:
        """Validate Delta table against quality rules"""
        df = self.spark.table(table_name)
        return self.validate_dataframe(df, rules, table_name)

# Common quality rules
COMMON_QUALITY_RULES = [
    QualityRule(
        name="no_nulls_in_primary_key",
        description="Primary key columns should not be null",
        sql_condition="id IS NOT NULL",
        error_threshold=0.0,
        severity="ERROR"
    ),
    QualityRule(
        name="valid_email_format",
        description="Email addresses should be valid",
        sql_condition="email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
        error_threshold=1.0,  # Allow 1% invalid emails
        severity="WARNING"
    ),
    QualityRule(
        name="reasonable_date_range",
        description="Dates should be within reasonable range",
        sql_condition="created_date BETWEEN '2020-01-01' AND current_date()",
        error_threshold=0.0,
        severity="ERROR"
    )
]
```

### 6. Testing Patterns

```python
# tests/conftest.py
import pytest
from unittest.mock import Mock, MagicMock
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

@pytest.fixture(scope="session")
def spark_session():
    """Create SparkSession for testing"""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

@pytest.fixture
def mock_databricks_client():
    """Mock Databricks client for unit tests"""
    client = Mock(spec=WorkspaceClient)
    client.catalogs.list.return_value = []
    client.schemas.list.return_value = []
    return client

@pytest.fixture
def sample_dataframe(spark_session):
    """Create sample DataFrame for testing"""
    data = [
        (1, "user1@example.com", "2023-01-01"),
        (2, "user2@example.com", "2023-01-02"),
        (3, "invalid-email", "2023-01-03")
    ]
    columns = ["id", "email", "created_date"]
    return spark_session.createDataFrame(data, columns)

# tests/test_delta_operations.py
import pytest
from unittest.mock import Mock, patch
from utils.delta_operations import DeltaTableManager

class TestDeltaTableManager:
    
    def test_create_table_if_not_exists_new_table(self, spark_session, sample_dataframe):
        """Test creating a new Delta table"""
        manager = DeltaTableManager(spark_session, "test_catalog", "test_schema")
        
        with patch.object(spark_session, 'sql') as mock_sql:
            # Simulate table doesn't exist
            mock_sql.side_effect = [Exception("Table not found"), None]
            
            with patch.object(sample_dataframe.write, 'saveAsTable') as mock_save:
                manager.create_table_if_not_exists("test_table", sample_dataframe)
                mock_save.assert_called_once()
    
    def test_data_quality_validation(self, spark_session, sample_dataframe):
        """Test data quality validation"""
        from utils.data_quality import DataQualityValidator, QualityRule
        
        validator = DataQualityValidator(spark_session)
        
        rules = [
            QualityRule(
                name="no_null_ids",
                description="ID should not be null",
                sql_condition="id IS NOT NULL",
                error_threshold=0.0
            )
        ]
        
        results = validator.validate_dataframe(sample_dataframe, rules)
        assert results["rules_passed"] == 1
        assert results["rules_failed"] == 0

# Integration test patterns
# tests/integration/test_databricks_integration.py
import pytest
from databricks.sdk import WorkspaceClient

@pytest.mark.integration
def test_databricks_connection():
    """Test actual Databricks connection"""
    client = WorkspaceClient()
    catalogs = list(client.catalogs.list())
    assert len(catalogs) >= 0  # Should not raise exception

@pytest.mark.integration
def test_delta_table_creation():
    """Test actual Delta table creation in Databricks"""
    # This test runs against actual Databricks workspace
    pass
```

### 7. Logging and Observability Pattern

```python
# utils/logging_config.py
import structlog
import logging
from datetime import datetime
from typing import Dict, Any

def configure_logging(level: str = "INFO") -> None:
    """Configure structured logging"""
    
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper())
    )
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

# utils/metrics.py
import time
from contextlib import contextmanager
from typing import Generator, Dict, Any
import structlog

logger = structlog.get_logger()

@contextmanager
def track_execution_time(operation_name: str, **context) -> Generator[Dict[str, Any], None, None]:
    """Context manager to track execution time"""
    start_time = time.time()
    metrics = {"operation": operation_name, **context}
    
    try:
        logger.info("Operation started", **metrics)
        yield metrics
        
    except Exception as e:
        metrics["error"] = str(e)
        metrics["success"] = False
        logger.error("Operation failed", **metrics)
        raise
        
    finally:
        end_time = time.time()
        metrics["duration_seconds"] = end_time - start_time
        metrics["success"] = metrics.get("success", True)
        
        logger.info("Operation completed", **metrics)

# Usage Example
with track_execution_time("delta_table_merge", table="users", records=1000) as metrics:
    # Perform Delta table merge operation
    merge_result = delta_manager.merge_incremental_data(...)
    metrics["records_merged"] = merge_result.get("num_target_rows_updated", 0)
```

### 8. Deployment and Configuration Pattern

```python
# deployment/asset_bundle_config.py
from typing import Dict, Any
import yaml
import os

class AssetBundleConfig:
    """Generate Databricks Asset Bundle configuration"""
    
    def __init__(self, project_name: str):
        self.project_name = project_name
    
    def generate_config(self, environments: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate databricks.yml configuration"""
        
        config = {
            "bundle": {
                "name": self.project_name
            },
            "workspace": {
                "root_path": "~/.bundle/${bundle.name}/${bundle.target}"
            },
            "artifacts": {
                "default": {
                    "path": "./src",
                    "type": "whl",
                    "build": "pip wheel -w dist ."
                }
            },
            "resources": {
                "jobs": self._generate_job_configs()
            },
            "environments": environments
        }
        
        return config
    
    def _generate_job_configs(self) -> Dict[str, Any]:
        """Generate job configurations"""
        return {
            "data_ingestion_job": {
                "name": "Data Ingestion Pipeline",
                "job_clusters": [{
                    "job_cluster_key": "main_cluster",
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 2,
                        "data_security_mode": "SINGLE_USER"
                    }
                }],
                "tasks": [{
                    "task_key": "ingest_data",
                    "job_cluster_key": "main_cluster",
                    "python_wheel_task": {
                        "package_name": self.project_name,
                        "entry_point": "ingest_data",
                        "parameters": ["--env", "${bundle.target}"]
                    }
                }]
            }
        }
    
    def write_config(self, filepath: str, environments: Dict[str, Dict[str, Any]]) -> None:
        """Write configuration to databricks.yml"""
        config = self.generate_config(environments)
        
        with open(filepath, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)

# Example usage
if __name__ == "__main__":
    environments = {
        "dev": {
            "workspace": {
                "host": os.getenv("DATABRICKS_DEV_URL")
            },
            "variables": {
                "catalog": "dev_catalog",
                "schema": "dev_schema"
            }
        },
        "prod": {
            "workspace": {
                "host": os.getenv("DATABRICKS_PROD_URL")
            },
            "variables": {
                "catalog": "prod_catalog",
                "schema": "prod_schema"
            }
        }
    }
    
    config_generator = AssetBundleConfig("data_platform")
    config_generator.write_config("databricks.yml", environments)
```

These patterns provide a solid foundation for building maintainable, testable, and scalable Databricks applications while following best practices for error handling, logging, and deployment.