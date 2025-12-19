# /Volumes/workspace/awin_products/raw_data/awin_feed_data/standard.csv.gz

import io
import json
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging
from typing import List
from databricks.sdk.runtime import spark
import requests
import warnings
from typing import Dict, Any, Optional


# Get logger for this module
logger = logging.getLogger(__name__)


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
    """
    Load a gzipped CSV file into a Spark DataFrame.
    
    Args:
        source_path: Path to the gzipped CSV file
        
    Returns:
        Spark DataFrame loaded from the CSV file
        
    Raises:
        Exception: If the file cannot be loaded
    """
    
    try:
        if source_url.startswith("http"):
            response = requests.get(source_url)
            response.raise_for_status()
            csv_data = io.BytesIO(response.content)
            pdf = pd.read_csv(csv_data, compression='gzip', low_memory=False)

            # df = spark.read.option("compression", "gzip").csv(source_url, header=True)
            df = spark.createDataFrame(pdf)
        else:
            df = spark.read.option("compression", "gzip").csv(source_url, header=True)
            
        logger.info(f"Successfully loaded CSV file from: {source_url}")
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV file from {source_url}: {str(e)}")
        raise


def _identify_all_null_columns(df: DataFrame) -> tuple[List[str], List[str]]:
    """
    Identify columns with all null values and columns with data.
    
    Args:
        df: Spark DataFrame to analyze
        
    Returns:
        Tuple of (all_null_columns, kept_columns)
    """
    total_count = df.count()
    
    if total_count == 0:
        logger.info("DataFrame is empty, no columns to analyze")
        return [], df.columns
    
    all_null_columns = []
    kept_columns = []
    
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count == total_count:
            all_null_columns.append(column)
        else:
            kept_columns.append(column)
    
    return all_null_columns, kept_columns


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
