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
    Classifies Brands based on the average price of their product portfolio.
    """
    global_median, global_p90 = get_global_price_statistics(spark, silver_catalog, silver_schema)
    
    sql = f"""
    SELECT
        s.brand_name,
        COUNT(i.aw_product_id) AS total_products,
        AVG(i.search_price) AS average_brand_price,
        CASE
            WHEN AVG(i.search_price) < {global_median} THEN 'Cheap'
            WHEN AVG(i.search_price) > {global_p90} THEN 'Lux'
            ELSE 'Medium'
        END AS brand_segment
    FROM
        {silver_catalog}.{silver_schema}.default_product_info i
    JOIN
        {silver_catalog}.{silver_schema}.product_specifications s ON i.aw_product_id = s.aw_product_id
    WHERE
        i.search_price IS NOT NULL AND s.brand_name IS NOT NULL
    GROUP BY
        s.brand_name
    """
    gold_dataframe = spark.sql(sql)
    
    return gold_dataframe


def run_main():
    try:
        main(standalone_mode=False)
    except Exception as e:
        print(f"Job failed with error: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    run_main()