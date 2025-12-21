import click
from pyspark.sql import DataFrame
from shared.persistence import cast_columns_from_schema, load_data_frame_from_table, load_raw_gzip_csv, load_table_schema, save_dataframe_with_metadata
import pyspark.sql.functions as F


@click.command()
@click.option("--metadata-path", type=click.STRING, envvar="METADATA_PATH", help="Path to the JSON metadata file")
@click.option("--write-mode", type=click.Choice(["overwrite", "append"]), envvar="WRITE_MODE", help="Spark write mode")
@click.option("--catalog", type=click.STRING, envvar="CATALOG", help="The catalog name")
@click.option("--bronze-schema", type=click.STRING, envvar="BRONZE_SCHEMA", help="The source bronze schema name")
@click.option("--bronze-table", type=click.STRING, envvar="BRONZE_TABLE", help="The source bronze table name")
@click.option("--silver-schema", type=click.STRING, envvar="SILVER_SCHEMA", help="The output silver schema name")
@click.option("--silver-table", type=click.STRING, envvar="SILVER_TABLE", help="The source silver table name")
def main(metadata_path: str, write_mode: str, catalog: str, bronze_schema: str, bronze_table: str, silver_schema: str, silver_table: str):
    schema_metadata = load_table_schema(metadata_path, silver_table)
    bronze_dataframe = load_data_frame_from_table(catalog, bronze_schema, bronze_table)
    
    silver_dataframe = _transform(bronze_dataframe)
            
    save_dataframe_with_metadata(silver_dataframe, catalog, silver_schema, silver_table, write_mode, schema_metadata)


def _transform(bronze_dataframe: DataFrame) -> DataFrame:
    columns_to_keep = [
        "aw_product_id",
        "merchant_product_id",

        "data_feed_id",
        "merchant_id",
        
        "merchant_name",
            
        "aw_image_url",
        "merchant_deep_link",

        "display_price",
    ]

    silver_dataframe = bronze_dataframe\
        .withColumn("display_price", F.replace(F.col("display_price"), F.lit("BRL"), F.lit("")).try_cast("double"))\
        .where(F.col("display_price").isNotNull())\
        .select(*columns_to_keep)
    
    return silver_dataframe


def run_main():
    try:
        main(standalone_mode=False)
    except Exception as e:
        print(f"Job failed with error: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    run_main()