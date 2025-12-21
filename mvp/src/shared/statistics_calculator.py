from pyspark.sql import SparkSession


def get_global_price_statistics(spark: SparkSession, silver_catalog: str, silver_schema: str):
    stats = spark.sql(f"""
        SELECT
            percentile_approx(search_price, 0.5) AS global_median,
            percentile_approx(search_price, 0.9) AS global_p90
        FROM {silver_catalog}.{silver_schema}.default_product_info
        WHERE search_price IS NOT NULL
    """).collect()[0]
    return stats['global_median'], stats['global_p90']
