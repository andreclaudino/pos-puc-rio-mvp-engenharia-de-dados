import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from shared.persistence import cast_columns_from_schema


def test_cast_columns_from_schema_sets_invalid_to_null(spark: SparkSession):
    """Test that cast_columns_from_schema sets invalid values to null instead of removing rows."""
    
    # Create test data with mixed valid and invalid values
    test_data = [
        ("1", "10.5", "valid"),      # Valid numeric values
        ("2", "20.3", "valid"),      # Valid numeric values
        ("invalid", "30.1", "invalid_int"),  # Invalid integer, valid decimal
        ("4", "invalid_decimal", "invalid_decimal"),  # Valid integer, invalid decimal
        ("5", "50.7", "valid"),      # Valid numeric values
        ("another_invalid", "not_a_number", "both_invalid"),  # Both invalid
    ]
    
    # Create DataFrame with string columns
    df = spark.createDataFrame(test_data, ["id", "price", "status"])
    
    # Schema metadata that defines the expected types
    schema_info = {
        "columns": {
            "id": {
                "data_type": "INTEGER",
                "description": "Product ID"
            },
            "price": {
                "data_type": "DECIMAL",
                "description": "Product price"
            },
            "status": {
                "data_type": "VARCHAR",
                "description": "Status"
            }
        }
    }
    
    # Apply the function
    result_df = cast_columns_from_schema(df, schema_info)
    
    # Verify that all rows are kept (no row removal)
    expected_count = 6  # All original rows should be kept
    actual_count = result_df.count()
    
    assert actual_count == expected_count, f"Expected {expected_count} rows, got {actual_count}"
    
    # Verify the data types were applied correctly
    schema = result_df.schema
    assert schema["id"].dataType == LongType()
    assert schema["price"].dataType == DoubleType()
    assert schema["status"].dataType == StringType()
    
    # Collect results to check null values
    result_data = result_df.collect()
    
    # Check that invalid values are set to null
    # Row 0: ("1", "10.5", "valid") - should remain valid
    # Row 1: ("2", "20.3", "valid") - should remain valid  
    # Row 2: ("invalid", "30.1", "invalid_int") - id should be null, price valid
    # Row 3: ("4", "invalid_decimal", "invalid_decimal") - id valid, price null
    # Row 4: ("5", "50.7", "valid") - should remain valid
    # Row 5: ("another_invalid", "not_a_number", "both_invalid") - both null
    
    assert result_data[0].id == 1 and result_data[0].price == 10.5  # Valid row
    assert result_data[1].id == 2 and result_data[1].price == 20.3  # Valid row
    assert result_data[2].id is None and result_data[2].price == 30.1  # Invalid id
    assert result_data[3].id == 4 and result_data[3].price is None  # Invalid price
    assert result_data[4].id == 5 and result_data[4].price == 50.7  # Valid row
    assert result_data[5].id is None and result_data[5].price is None  # Both invalid


def test_cast_columns_from_schema_no_schema(spark: SparkSession):
    """Test that function returns original DataFrame when no schema is provided."""
    
    test_data = [("1", "10.5"), ("2", "20.3")]
    df = spark.createDataFrame(test_data, ["id", "price"])
    
    original_count = df.count()
    
    # Apply with None schema
    result_df = cast_columns_from_schema(df, None)
    
    # Should return the same DataFrame
    assert result_df.count() == original_count
    assert result_df.schema == df.schema


def test_cast_columns_from_schema_empty_schema(spark: SparkSession):
    """Test that function returns original DataFrame when empty schema is provided."""
    
    test_data = [("1", "10.5"), ("2", "20.3")]
    df = spark.createDataFrame(test_data, ["id", "price"])
    
    original_count = df.count()
    
    # Apply with empty schema
    result_df = cast_columns_from_schema(df, {})
    
    # Should return the same DataFrame
    assert result_df.count() == original_count
    assert result_df.schema == df.schema


def test_cast_columns_from_schema_no_cast_errors(spark: SparkSession):
    """Test that function keeps all rows when there are no cast errors."""
    
    # All valid data
    test_data = [
        ("1", "10.5", "valid"),
        ("2", "20.3", "valid"),
        ("5", "50.7", "valid"),
    ]
    
    df = spark.createDataFrame(test_data, ["id", "price", "status"])
    
    schema_info = {
        "columns": {
            "id": {"data_type": "INTEGER"},
            "price": {"data_type": "DECIMAL"},
            "status": {"data_type": "VARCHAR"}
        }
    }
    
    result_df = cast_columns_from_schema(df, schema_info)
    
    # Should keep all rows
    assert result_df.count() == 3
    
    # Verify types were applied
    schema = result_df.schema
    assert schema["id"].dataType == LongType()
    assert schema["price"].dataType == DoubleType()
    assert schema["status"].dataType == StringType()
