import click
from pyspark.sql import DataFrame
from shared.persistence import load_table_schema, save_dataframe_with_metadata
import pyspark.sql.functions as F
from databricks.sdk.runtime import spark


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
    Transformation logic for category_price_classification table.
    Calculates Cheap/Medium/Lux percentages based on global statistics.
    """
    
    # Cálculo das estatísticas globais para uso na query principal
    # Em tasks ETL, é mais seguro fazer isso separado para garantir que o spark capture os valores escalares
    stats_query = f"""
    SELECT
        percentile_approx(search_price, 0.5) AS global_median,
        percentile_approx(search_price, 0.9) AS global_p90
    FROM {silver_catalog}.{silver_schema}.default_product_info
    WHERE search_price IS NOT NULL
    """
    
    # Coletamos os valores para injetar na query principal (evita cross join complexo em datasets gigantes)
    stats_row = spark.sql(stats_query).collect()[0]
    global_median = stats_row['global_median']
    global_p90 = stats_row['global_p90']

    sql = f"""
    SELECT
        category,
        COUNT(*) AS total_products,
        
        -- Counts
        COUNT(CASE WHEN search_price > {global_p90} THEN 1 END) AS lux_count,
        COUNT(CASE WHEN search_price >= {global_median} AND search_price <= {global_p90} THEN 1 END) AS medium_count,
        COUNT(CASE WHEN search_price < {global_median} THEN 1 END) AS cheap_count,
        
        -- Percentages
        (COUNT(CASE WHEN search_price > {global_p90} THEN 1 END) / COUNT(*)) * 100 AS lux_percentage,
        (COUNT(CASE WHEN search_price >= {global_median} AND search_price <= {global_p90} THEN 1 END) / COUNT(*)) * 100 AS medium_percentage,
        (COUNT(CASE WHEN search_price < {global_median} THEN 1 END) / COUNT(*)) * 100 AS cheap_percentage
        
    FROM
        {silver_catalog}.{silver_schema}.default_product_info
    WHERE
        search_price IS NOT NULL
    GROUP BY
        category
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