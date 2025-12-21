import click
from pyspark.sql import DataFrame
from shared.persistence import load_table_schema, save_dataframe_with_metadata
import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from shared.statistics_calculator import get_global_price_statistics


@click.command()
@click.option("--metadata-path", type=click.STRING, envvar="METADATA_PATH", help="Path to the JSON metadata file")
@click.option("--write-mode", type=click.Choice(["overwrite", "append"]), envvar="WRITE_MODE", help="Spark write mode")
@click.option("--catalog", type=click.STRING, envvar="CATALOG", help="The catalog name")
@click.option("--silver-schema", type=click.STRING, envvar="SILVER_SCHEMA", help="The source silver schema name")
@click.option("--gold-schema", type=click.STRING, envvar="BRONZE_SCHEMA", help="The output gold schema name")
@click.option("--gold-table", type=click.STRING, envvar="BRONZE_TABLE", help="The output gold table name")
def main(metadata_path: str, write_mode: str, catalog: str, silver_schema: str, gold_schema: str, gold_table: str):
    schema_metadata = load_table_schema(metadata_path, gold_table)
    silver_dataframe = _transform(catalog, silver_schema)
            
    save_dataframe_with_metadata(silver_dataframe, catalog, gold_schema, gold_table,
                                 write_mode, schema_metadata)


def _transform(silver_catalog: str, silver_schema: str) -> DataFrame:
    """
    Classifies 'Categories' (categories defined by the seller, not Awin) based on average price.
    """
    global_median, global_p90 = get_global_price_statistics(spark, silver_catalog, silver_schema)
    
    sql = f"""
    SELECT
        category,
        COUNT(aw_product_id) AS total_products,
        AVG(search_price) AS average_category_price,
        CASE
            WHEN AVG(search_price) < {global_median} THEN 'Cheap'
            WHEN AVG(search_price) > {global_p90} THEN 'Lux'
            ELSE 'Medium'
        END AS category_segment
    FROM
        {silver_catalog}.{silver_schema}.default_product_info
    WHERE
        search_price IS NOT NULL AND category IS NOT NULL
    GROUP BY
        category
    """
    df = spark.sql(sql)
    
    return df


def run_main():
    try:
        main(standalone_mode=False)
    except Exception as e:
        print(f"Job failed with error: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    run_main()