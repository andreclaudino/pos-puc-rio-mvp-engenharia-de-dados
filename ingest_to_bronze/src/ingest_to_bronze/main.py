from databricks.sdk.runtime import spark
import click


@click.command()
@click.option("--source-path", type=click.STRING, help="The path to the raw file")
@click.option("--catalog", type=click.STRING, help="The output catalog")
@click.option("--schema", type=click.STRING, help="The output schema")
@click.option("--table", type=click.STRING, help="The output table")
def main(source_path: str, catalog: str, schema: str, table: str) -> None:

    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")

    



if __name__ == "__main__":
    main()
