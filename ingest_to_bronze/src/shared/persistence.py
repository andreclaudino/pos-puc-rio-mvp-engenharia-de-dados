# /Volumes/workspace/awin_products/raw_data/awin_feed_data/standard.csv.gz

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
import logging
from typing import List

# Get logger for this module
logger = logging.getLogger(__name__)


def load_raw_gzip_csv(source_path: str, remove_all_null: bool) -> DataFrame:
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
    df = _load_csv_file(source_path)
    
    # Remove all-null columns if requested
    if remove_all_null:
        df = _remove_all_null_columns(df)
    
    return df


def _load_csv_file(source_path: str) -> DataFrame:
    """
    Load a gzipped CSV file into a Spark DataFrame.
    
    Args:
        source_path: Path to the gzipped CSV file
        
    Returns:
        Spark DataFrame loaded from the CSV file
        
    Raises:
        Exception: If the file cannot be loaded
    """
    spark = SparkSession.builder.getOrCreate()
    
    try:
        df = spark.read.option("compression", "gzip").csv(source_path, header=True)
        logger.info(f"Successfully loaded CSV file from: {source_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV file from {source_path}: {str(e)}")
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

