# /Volumes/workspace/awin_products/raw_data/awin_feed_data/standard.csv.gz

from pyspark.sql import functions as F
import json
from pyspark.sql import DataFrame
import logging
from typing import List
from databricks.sdk.runtime import spark
import warnings
from typing import Dict, Any, Optional


# Get logger for this module
logger = logging.getLogger(__name__)


def load_data_frame_from_table(catalog: str, schema: str, table: str) -> DataFrame:
    dataframe = spark.table(f"{catalog}.{schema}.{table}")
    return dataframe


def save_dataframe_with_metadata(
    data_frame: DataFrame,
    catalog: str,
    schema_name: str,
    table_name: str,
    write_mode: str,
    schema_info: Optional[Dict[str, Any]] = None
) -> None:
    """
    Main function to save data and enrich with metadata.
    """
    full_table_name = f"{catalog}.{schema_name}.{table_name}"

    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    
    # Call private save function
    _save_to_catalog(data_frame, full_table_name, write_mode)
    logger.info(f"table {full_table_name} Loaded successfully")
    
    # Call private comment function
    if schema_info:
        _apply_metadata_comments(full_table_name, data_frame.columns, schema_info)
        logger.info(f"table metadata {full_table_name} applied successfully")
    else:
        warnings.warn(f"No schema info provided for {full_table_name}. Skipping comments.")


def _save_to_catalog(df: DataFrame, full_table_name: str, write_mode: str) -> None:
    """
    Private function focused only on the I/O operation.
    """
    df.write.mode(write_mode).saveAsTable(full_table_name)


def _apply_metadata_comments(full_table_name: str, df_columns: list, schema_info: Dict[str, Any]) -> None:
    """
    Private function focused only on applying SQL comments to the table and columns.
    """
    # 1. Apply Table Comment
    table_desc = schema_info.get("table_description")
    if table_desc:
        escaped_table_desc = table_desc.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{escaped_table_desc}'")
    
    # 2. Apply Column Comments
    metadata_cols = schema_info.get("columns", {})
    for col_name in df_columns:
        if col_name in metadata_cols:
            col_desc = metadata_cols[col_name].get("description")
            if col_desc:
                escaped_col_desc = col_desc.replace("'", "''")
                spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {col_name} COMMENT '{escaped_col_desc}'")


def load_table_schema(file_path: str, table_name: str) -> Optional[Dict[str, Any]]:
    """
    Loads the schema of a specific table from the Awin JSON configuration file.
    
    Args:
        file_path (str): The path to the JSON schema file.
        table_name (str): The specific table key (e.g., 'pricing_and_savings').
        
    Returns:
        dict: A dictionary containing 'table_description' and 'columns', 
              or None if the table is not found.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            schema_data = json.load(file)
        
        # Access the specific table using the key
        table_data = schema_data.get(table_name)
        
        if table_data:
            return table_data
        else:
            available_tables = list(schema_data.keys())
            raise ValueError(f"Table '{table_name}' not found in metadata file {file_path}. Available tables: {available_tables}")

    except FileNotFoundError:
        logger.error(f"The file at {file_path} was not found.")
        raise
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON. Please check the file syntax.")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise

def load_raw_gzip_csv(source_url: str, remove_all_null: bool) -> DataFrame:
    """
    Load a Spark DataFrame from the given source_path. The DataFrame
    is in CSV format with Gzip.

    if remove_all_null is True, columns that have all
    values null are removed.
    
    Args:
        source_path: Path to the gzipped CSV file
        remove_all_null: Whether to remove columns with all null values
        
    Returns:
        Spark DataFrame loaded from the CSV file
        
    Raises:
        Exception: If the file cannot be loaded
    """
        
    # Load the CSV file
    df = _load_csv_file(source_url)
    
    # Remove all-null columns if requested
    if remove_all_null:
        df = _remove_all_null_columns(df)
    
    return df


def _load_csv_file(source_url: str) -> DataFrame:
    try:
        if source_url.startswith("http"):
            # Spark não lê HTTP nativamente bem para CSV Gzip.
            # Alternativa performática: Baixar via shell para o armazenamento temporário
            # Isso evita estourar a RAM do Python/Driver.
            temp_path = "/tmp/awin_feed_temp.csv.gz"
            import subprocess
            subprocess.run(["wget", "-O", temp_path, source_url], check=True)
            source_url = f"file:{temp_path}"

        # Leitura nativa Spark (Distribuída)
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("compression", "gzip") \
            .csv(source_url, sep="|")
            
        logger.info(f"Successfully loaded CSV file from: {source_url}")
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV file from {source_url}: {str(e)}")
        raise

def _identify_all_null_columns(df: DataFrame) -> tuple[List[str], List[str]]:
    total_count = df.count()
    if total_count == 0:
        return [], df.columns

    # Cria uma lista de agregações: count_if(col is null) para cada coluna
    null_counts_expr = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    
    # Executa UM único Job Spark para a tabela toda
    null_counts_row = df.select(null_counts_expr).collect()[0]
    
    all_null_columns = [c for c in df.columns if null_counts_row[c] == total_count]
    kept_columns = [c for c in df.columns if null_counts_row[c] < total_count]
    
    return all_null_columns, kept_columns


def cast_columns_from_schema(df: DataFrame, schema_info: Optional[Dict[str, Any]]) -> DataFrame:
    if not schema_info:
        return df
    
    metadata_cols = schema_info.get("columns", {})
    casted_columns = []
    
    # Store original count for logging
    original_count = df.count()
    
    # Perform safe casting using regex validation - set invalid values to null
    for col_name in df.columns:
        if col_name in metadata_cols:
            target_type = metadata_cols[col_name].get("data_type", "").upper()
            
            # Create safe casting using regex validation
            if "VARCHAR" in target_type or "TEXT" in target_type:
                # String casts generally don't fail, but we handle them consistently
                df = df.withColumn(col_name, F.col(col_name).cast("string"))
                casted_columns.append(col_name)
            elif "INTEGER" in target_type or "BIGINT" in target_type:
                # For integer types: check if value matches integer pattern, cast if valid, null otherwise
                df = df.withColumn(col_name, 
                    F.when(F.col(col_name).rlike(r'^-?\d+$'), F.col(col_name).cast("long"))
                     .otherwise(None))
                casted_columns.append(col_name)
            elif "DECIMAL" in target_type:
                # For decimal types: check if value matches decimal pattern, cast if valid, null otherwise
                df = df.withColumn(col_name,
                    F.when(F.col(col_name).rlike(r'^-?\d*\.?\d+$'), F.col(col_name).cast("double"))
                     .otherwise(None))
                casted_columns.append(col_name)
    
    # Log the results but keep all rows
    if casted_columns:
        # Count null values in casted columns to report cast errors
        null_counts = []
        for col_name in casted_columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count > 0:
                null_counts.append(f"{col_name}: {null_count}")
        
        if null_counts:
            logger.warning(f"Set null values for invalid data in columns: {', '.join(null_counts)}")
            logger.info(f"Total rows processed: {original_count} (all rows retained)")
        else:
            logger.info(f"All casts successful. Total rows processed: {original_count}")
                
    return df


def _remove_all_null_columns(df: DataFrame) -> DataFrame:
    """
    Remove columns with all null values from DataFrame and log the results.
    
    Args:
        df: Spark DataFrame to process
        
    Returns:
        DataFrame with all-null columns removed
    """
    all_null_columns, kept_columns = _identify_all_null_columns(df)
    
    if all_null_columns:
        logger.info(f"Removed columns with all null values: {all_null_columns}")
        df = df.drop(*all_null_columns)
    
    if kept_columns:
        logger.info(f"Kept columns with data: {kept_columns}")
    
    return df
