"""
Enterprise Data Platform - Implementation Examples
Demonstrates composition-based, Databricks-native patterns
"""

from abc import ABC, abstractmethod
from typing import Protocol, Dict, List, Optional, Any
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
import logging


# =============================================================================
# Core Protocols and Interfaces
# =============================================================================

class SourceAdapter(Protocol):
    """Protocol for data source adapters"""
    
    def connect(self) -> bool:
        """Establish connection to the data source"""
        ...
    
    def read_table(self, table_name: str) -> DataFrame:
        """Read a complete table"""
        ...
    
    def read_incremental(self, table_name: str, watermark_column: str, 
                        last_value: Any) -> DataFrame:
        """Read incremental changes since last extraction"""
        ...
    
    def get_schema(self, table_name: str) -> StructType:
        """Get table schema information"""
        ...
    
    def test_connection(self) -> bool:
        """Test if connection is healthy"""
        ...


class DataProcessor(Protocol):
    """Protocol for data processing components"""
    
    def process(self, df: DataFrame) -> DataFrame:
        """Process the input DataFrame"""
        ...
    
    def validate_input(self, df: DataFrame) -> bool:
        """Validate input data meets processor requirements"""
        ...


class DataValidator(Protocol):
    """Protocol for data validation components"""
    
    def validate(self, df: DataFrame) -> 'ValidationResult':
        """Validate data quality"""
        ...


class DataWriter(Protocol):
    """Protocol for data writing components"""
    
    def write(self, df: DataFrame, destination: str, mode: str = "overwrite") -> None:
        """Write DataFrame to destination"""
        ...


# =============================================================================
# Data Models and Configuration
# =============================================================================

@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    error_count: int
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, Any]


@dataclass
class OracleConfig:
    """Configuration for Oracle database connection"""
    host: str
    port: int
    service_name: str
    username: str
    password_secret: str
    
    def to_jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.service_name}"


@dataclass
class PipelineConfig:
    """Configuration for data pipeline"""
    source_config: Dict[str, Any]
    processing_config: Dict[str, Any]
    destination_config: Dict[str, Any]
    quality_config: Dict[str, Any]


# =============================================================================
# Source Adapter Implementations
# =============================================================================

class OracleAdapter:
    """Oracle database adapter using Databricks JDBC"""
    
    def __init__(self, config: OracleConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """Test connection to Oracle database"""
        try:
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.to_jdbc_url()) \
                .option("query", "SELECT 1 FROM DUAL") \
                .option("user", self.config.username) \
                .option("password", self._get_password()) \
                .load()
            
            test_df.collect()  # Force execution
            return True
        except Exception as e:
            self.logger.error(f"Oracle connection failed: {e}")
            return False
    
    def read_table(self, table_name: str) -> DataFrame:
        """Read complete table from Oracle"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config.to_jdbc_url()) \
            .option("dbtable", table_name) \
            .option("user", self.config.username) \
            .option("password", self._get_password()) \
            .option("fetchsize", "10000") \
            .load()
    
    def read_incremental(self, table_name: str, watermark_column: str, 
                        last_value: Any) -> DataFrame:
        """Read incremental changes from Oracle"""
        query = f"""
        (SELECT * FROM {table_name} 
         WHERE {watermark_column} > '{last_value}') AS incremental_data
        """
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config.to_jdbc_url()) \
            .option("dbtable", query) \
            .option("user", self.config.username) \
            .option("password", self._get_password()) \
            .option("fetchsize", "10000") \
            .load()
    
    def get_schema(self, table_name: str) -> StructType:
        """Get table schema from Oracle"""
        sample_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config.to_jdbc_url()) \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE ROWNUM = 1)") \
            .option("user", self.config.username) \
            .option("password", self._get_password()) \
            .load()
        
        return sample_df.schema
    
    def test_connection(self) -> bool:
        """Test if connection is healthy"""
        return self.connect()
    
    def _get_password(self) -> str:
        """Get password from Databricks secrets"""
        return dbutils.secrets.get("oracle", self.config.password_secret)


class SQLServerAdapter:
    """SQL Server adapter using Databricks JDBC"""
    
    def __init__(self, server: str, database: str, username: str, 
                 password_secret: str, spark: SparkSession):
        self.server = server
        self.database = database
        self.username = username
        self.password_secret = password_secret
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """Test connection to SQL Server"""
        try:
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("query", "SELECT 1") \
                .option("user", self.username) \
                .option("password", self._get_password()) \
                .load()
            
            test_df.collect()
            return True
        except Exception as e:
            self.logger.error(f"SQL Server connection failed: {e}")
            return False
    
    def read_table(self, table_name: str) -> DataFrame:
        """Read complete table from SQL Server"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", table_name) \
            .option("user", self.username) \
            .option("password", self._get_password()) \
            .load()
    
    def read_incremental(self, table_name: str, watermark_column: str, 
                        last_value: Any) -> DataFrame:
        """Read incremental changes from SQL Server"""
        query = f"""
        (SELECT * FROM {table_name} 
         WHERE {watermark_column} > '{last_value}') AS incremental_data
        """
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", query) \
            .option("user", self.username) \
            .option("password", self._get_password()) \
            .load()
    
    def get_schema(self, table_name: str) -> StructType:
        """Get table schema from SQL Server"""
        sample_df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", f"(SELECT TOP 1 * FROM {table_name}) AS sample") \
            .option("user", self.username) \
            .option("password", self._get_password()) \
            .load()
        
        return sample_df.schema
    
    def test_connection(self) -> bool:
        """Test if connection is healthy"""
        return self.connect()
    
    def _get_jdbc_url(self) -> str:
        """Build JDBC URL for SQL Server"""
        return f"jdbc:sqlserver://{self.server};databaseName={self.database}"
    
    def _get_password(self) -> str:
        """Get password from Databricks secrets"""
        return dbutils.secrets.get("sqlserver", self.password_secret)


# =============================================================================
# Data Processing Components
# =============================================================================

class DataCleaningProcessor:
    """Composition-based data cleaning processor"""
    
    def __init__(self, cleaning_rules: List['CleaningRule']):
        self.cleaning_rules = cleaning_rules
        self.logger = logging.getLogger(__name__)
    
    def process(self, df: DataFrame) -> DataFrame:
        """Apply all cleaning rules to the DataFrame"""
        result_df = df
        
        for rule in self.cleaning_rules:
            self.logger.info(f"Applying cleaning rule: {rule.__class__.__name__}")
            result_df = rule.apply(result_df)
        
        return result_df
    
    def validate_input(self, df: DataFrame) -> bool:
        """Validate input data meets processor requirements"""
        return df is not None and df.count() > 0
    
    def add_rule(self, rule: 'CleaningRule') -> None:
        """Add a new cleaning rule"""
        self.cleaning_rules.append(rule)


class CleaningRule(ABC):
    """Abstract base class for cleaning rules"""
    
    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """Apply the cleaning rule to the DataFrame"""
        pass


class RemoveNullsRule(CleaningRule):
    """Remove rows with null values in specified columns"""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    def apply(self, df: DataFrame) -> DataFrame:
        """Remove rows with nulls in specified columns"""
        return df.dropna(subset=self.columns)


class StandardizeTextRule(CleaningRule):
    """Standardize text fields (trim, uppercase, etc.)"""
    
    def __init__(self, columns: List[str], operation: str = "trim"):
        self.columns = columns
        self.operation = operation
    
    def apply(self, df: DataFrame) -> DataFrame:
        """Apply text standardization"""
        from pyspark.sql.functions import trim, upper, lower
        
        result_df = df
        for col in self.columns:
            if self.operation == "trim":
                result_df = result_df.withColumn(col, trim(result_df[col]))
            elif self.operation == "upper":
                result_df = result_df.withColumn(col, upper(result_df[col]))
            elif self.operation == "lower":
                result_df = result_df.withColumn(col, lower(result_df[col]))
        
        return result_df


class DataEnrichmentProcessor:
    """Composition-based data enrichment processor"""
    
    def __init__(self, enrichment_sources: Dict[str, 'EnrichmentSource']):
        self.enrichment_sources = enrichment_sources
        self.logger = logging.getLogger(__name__)
    
    def process(self, df: DataFrame) -> DataFrame:
        """Apply all enrichments to the DataFrame"""
        result_df = df
        
        for name, source in self.enrichment_sources.items():
            self.logger.info(f"Applying enrichment: {name}")
            result_df = source.enrich(result_df)
        
        return result_df
    
    def validate_input(self, df: DataFrame) -> bool:
        """Validate input data meets processor requirements"""
        return df is not None and df.count() > 0
    
    def add_enrichment(self, name: str, source: 'EnrichmentSource') -> None:
        """Add a new enrichment source"""
        self.enrichment_sources[name] = source


class EnrichmentSource(ABC):
    """Abstract base class for enrichment sources"""
    
    @abstractmethod
    def enrich(self, df: DataFrame) -> DataFrame:
        """Enrich the DataFrame with additional data"""
        pass


class LookupTableEnrichment(EnrichmentSource):
    """Enrich data using a lookup table"""
    
    def __init__(self, lookup_table: str, join_column: str, 
                 select_columns: List[str], spark: SparkSession):
        self.lookup_table = lookup_table
        self.join_column = join_column
        self.select_columns = select_columns
        self.spark = spark
    
    def enrich(self, df: DataFrame) -> DataFrame:
        """Join with lookup table to enrich data"""
        lookup_df = self.spark.table(self.lookup_table).select(
            [self.join_column] + self.select_columns
        )
        
        return df.join(lookup_df, on=self.join_column, how="left")


# =============================================================================
# Data Quality Framework
# =============================================================================

class DataQualityValidator:
    """Comprehensive data quality validation"""
    
    def __init__(self, expectations: List['DataExpectation']):
        self.expectations = expectations
        self.logger = logging.getLogger(__name__)
    
    def validate(self, df: DataFrame) -> ValidationResult:
        """Run all data quality expectations"""
        errors = []
        warnings = []
        metrics = {}
        total_rows = df.count()
        
        for expectation in self.expectations:
            try:
                result = expectation.check(df)
                
                if not result.is_valid:
                    if result.severity == "error":
                        errors.extend(result.messages)
                    else:
                        warnings.extend(result.messages)
                
                metrics[expectation.name] = result.metrics
                
            except Exception as e:
                self.logger.error(f"Error running expectation {expectation.name}: {e}")
                errors.append(f"Expectation {expectation.name} failed: {e}")
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            error_count=len(errors),
            errors=errors,
            warnings=warnings,
            metrics=metrics
        )


@dataclass 
class ExpectationResult:
    """Result of a single data expectation"""
    is_valid: bool
    severity: str  # "error" or "warning"
    messages: List[str]
    metrics: Dict[str, Any]


class DataExpectation(ABC):
    """Abstract base class for data expectations"""
    
    def __init__(self, name: str, severity: str = "error"):
        self.name = name
        self.severity = severity
    
    @abstractmethod
    def check(self, df: DataFrame) -> ExpectationResult:
        """Check the expectation against the DataFrame"""
        pass


class ExpectNonNullValues(DataExpectation):
    """Expect no null values in specified columns"""
    
    def __init__(self, columns: List[str], threshold: float = 0.0):
        super().__init__(f"non_null_{','.join(columns)}")
        self.columns = columns
        self.threshold = threshold
    
    def check(self, df: DataFrame) -> ExpectationResult:
        """Check for null values"""
        messages = []
        metrics = {}
        total_rows = df.count()
        
        for col in self.columns:
            null_count = df.filter(df[col].isNull()).count()
            null_rate = null_count / total_rows if total_rows > 0 else 0
            
            metrics[f"{col}_null_count"] = null_count
            metrics[f"{col}_null_rate"] = null_rate
            
            if null_rate > self.threshold:
                messages.append(
                    f"Column {col} has {null_count} null values "
                    f"({null_rate:.2%}), exceeds threshold {self.threshold:.2%}"
                )
        
        return ExpectationResult(
            is_valid=len(messages) == 0,
            severity=self.severity,
            messages=messages,
            metrics=metrics
        )


class ExpectUniqueValues(DataExpectation):
    """Expect unique values in specified columns"""
    
    def __init__(self, columns: List[str]):
        super().__init__(f"unique_{','.join(columns)}")
        self.columns = columns
    
    def check(self, df: DataFrame) -> ExpectationResult:
        """Check for duplicate values"""
        messages = []
        metrics = {}
        
        total_rows = df.count()
        unique_rows = df.select(self.columns).distinct().count()
        duplicate_count = total_rows - unique_rows
        
        metrics["total_rows"] = total_rows
        metrics["unique_rows"] = unique_rows
        metrics["duplicate_count"] = duplicate_count
        
        if duplicate_count > 0:
            messages.append(
                f"Found {duplicate_count} duplicate rows based on columns {self.columns}"
            )
        
        return ExpectationResult(
            is_valid=duplicate_count == 0,
            severity=self.severity,
            messages=messages,
            metrics=metrics
        )


# =============================================================================
# Unity Catalog Integration
# =============================================================================

class UnityGateway:
    """Interface to Unity Catalog for governance operations"""
    
    def __init__(self, catalog_name: str, spark: SparkSession):
        self.catalog_name = catalog_name
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def register_table(self, schema_name: str, table_name: str, 
                      location: str, comment: str = None) -> None:
        """Register a Delta table in Unity Catalog"""
        full_table_name = f"{self.catalog_name}.{schema_name}.{table_name}"
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING DELTA
        LOCATION '{location}'
        """
        
        if comment:
            sql += f" COMMENT '{comment}'"
        
        self.spark.sql(sql)
        self.logger.info(f"Registered table: {full_table_name}")
    
    def apply_row_level_security(self, schema_name: str, table_name: str,
                                policy_name: str, condition: str,
                                principals: List[str]) -> None:
        """Apply row-level security policy"""
        full_table_name = f"{self.catalog_name}.{schema_name}.{table_name}"
        principals_str = "', '".join(principals)
        
        sql = f"""
        CREATE OR REPLACE ROW ACCESS POLICY {policy_name}
        ON {full_table_name}
        GRANT TO ('{principals_str}')
        FILTER USING ({condition})
        """
        
        self.spark.sql(sql)
        self.logger.info(f"Applied RLS policy {policy_name} to {full_table_name}")
    
    def set_table_properties(self, schema_name: str, table_name: str,
                           properties: Dict[str, str]) -> None:
        """Set table properties for governance"""
        full_table_name = f"{self.catalog_name}.{schema_name}.{table_name}"
        
        for key, value in properties.items():
            sql = f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('{key}' = '{value}')"
            self.spark.sql(sql)
        
        self.logger.info(f"Set properties for {full_table_name}: {properties}")


# =============================================================================
# Pipeline Orchestration
# =============================================================================

class DataPipeline:
    """Orchestrates the complete data pipeline using composition"""
    
    def __init__(self, 
                 source_adapter: SourceAdapter,
                 processors: List[DataProcessor],
                 validator: DataValidator,
                 writer: DataWriter,
                 unity_gateway: UnityGateway):
        self.source_adapter = source_adapter
        self.processors = processors
        self.validator = validator
        self.writer = writer
        self.unity_gateway = unity_gateway
        self.logger = logging.getLogger(__name__)
    
    def execute(self, source_table: str, destination: str) -> bool:
        """Execute the complete pipeline"""
        try:
            # Test source connection
            if not self.source_adapter.test_connection():
                raise Exception("Source connection test failed")
            
            # Read source data
            self.logger.info(f"Reading data from {source_table}")
            source_df = self.source_adapter.read_table(source_table)
            
            # Process data through all processors
            processed_df = source_df
            for i, processor in enumerate(self.processors):
                self.logger.info(f"Running processor {i+1}/{len(self.processors)}")
                
                if not processor.validate_input(processed_df):
                    raise Exception(f"Processor {i+1} input validation failed")
                
                processed_df = processor.process(processed_df)
            
            # Validate final data quality
            self.logger.info("Running data quality validation")
            validation_result = self.validator.validate(processed_df)
            
            if not validation_result.is_valid:
                self.logger.error(f"Data quality validation failed: {validation_result.errors}")
                return False
            
            if validation_result.warnings:
                self.logger.warning(f"Data quality warnings: {validation_result.warnings}")
            
            # Write to destination
            self.logger.info(f"Writing data to {destination}")
            self.writer.write(processed_df, destination)
            
            self.logger.info("Pipeline execution completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            return False


# =============================================================================
# Example Usage and Factory Pattern
# =============================================================================

class PipelineFactory:
    """Factory for creating pre-configured pipelines"""
    
    @staticmethod
    def create_oracle_to_delta_pipeline(oracle_config: OracleConfig,
                                      catalog_name: str,
                                      spark: SparkSession) -> DataPipeline:
        """Create a pipeline for Oracle to Delta Lake"""
        
        # Create components
        source_adapter = OracleAdapter(oracle_config, spark)
        
        cleaning_rules = [
            RemoveNullsRule(["id", "created_date"]),
            StandardizeTextRule(["name", "description"], "trim")
        ]
        cleaning_processor = DataCleaningProcessor(cleaning_rules)
        
        enrichment_sources = {
            "customer_lookup": LookupTableEnrichment(
                "dim_customers", "customer_id", ["customer_name", "region"], spark
            )
        }
        enrichment_processor = DataEnrichmentProcessor(enrichment_sources)
        
        expectations = [
            ExpectNonNullValues(["id", "created_date"]),
            ExpectUniqueValues(["id"])
        ]
        validator = DataQualityValidator(expectations)
        
        writer = DeltaWriter()
        unity_gateway = UnityGateway(catalog_name, spark)
        
        # Compose pipeline
        return DataPipeline(
            source_adapter=source_adapter,
            processors=[cleaning_processor, enrichment_processor],
            validator=validator,
            writer=writer,
            unity_gateway=unity_gateway
        )


class DeltaWriter:
    """Writer implementation for Delta Lake"""
    
    def write(self, df: DataFrame, destination: str, mode: str = "overwrite") -> None:
        """Write DataFrame to Delta table"""
        df.write \
          .format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .saveAsTable(destination)


# =============================================================================
# Usage Example
# =============================================================================

def main():
    """Example usage of the enterprise data platform components"""
    
    # Initialize Spark session (in Databricks, this is automatically available)
    spark = SparkSession.builder.appName("EnterpriseDataPlatform").getOrCreate()
    
    # Configure Oracle connection
    oracle_config = OracleConfig(
        host="oracle-server.company.com",
        port=1521,
        service_name="PROD",
        username="etl_user",
        password_secret="oracle-password"
    )
    
    # Create pipeline using factory
    pipeline = PipelineFactory.create_oracle_to_delta_pipeline(
        oracle_config=oracle_config,
        catalog_name="enterprise_catalog",
        spark=spark
    )
    
    # Execute pipeline
    success = pipeline.execute(
        source_table="SALES.TRANSACTIONS",
        destination="enterprise_catalog.refined.sales_transactions"
    )
    
    if success:
        print("Pipeline execution completed successfully")
    else:
        print("Pipeline execution failed")


if __name__ == "__main__":
    main()