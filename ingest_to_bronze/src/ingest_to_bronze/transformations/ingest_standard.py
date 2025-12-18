import sys
import click
from shared.persistence import load_raw_gzip_csv, load_table_schema, save_dataframe_with_metadata


@click.command()
@click.option("--source-feed-url", type=click.STRING, envvar="SOURCE_PATH", help="The Feed url")
@click.option("--remove-all-null-columns/--keep-all-null-columns", envvar="REMOVE_ALL_NULL_COLUMNS", type=click.BOOL,
              default=True,
              help="Should remove all null columns")
@click.option("--metadata-path", type=click.STRING, envvar="METADATA_PATH", help="Path to the JSON metadata file")
@click.option("--write-mode", type=click.Choice(["overwrite", "append"]), envvar="WRITE_MODE", help="Spark write mode")
@click.option("--catalog", type=click.STRING, envvar="CATALOG", help="The output catalog name")
@click.option("--schema", type=click.STRING, envvar="SCHEMA", help="The output schema name")
@click.option("--table", type=click.STRING, envvar="TABLE", help="The output table name")
def main(source_feed_url: str, remove_all_null_columns: bool, metadata_path: str, write_mode: str, catalog: str, schema: str, table: str):
    schema_metadata = load_table_schema(metadata_path, table)
    raw_dataframe = load_raw_gzip_csv(source_feed_url, remove_all_null_columns)
    
    save_dataframe_with_metadata(raw_dataframe, catalog, schema, table, write_mode, schema_metadata)   

    return True

