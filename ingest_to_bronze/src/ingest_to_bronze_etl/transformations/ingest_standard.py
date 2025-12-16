from databricks.sdk.runtime import spark
import dlt

from ingest_to_bronze.src.shared.persistence import load_raw_gzip_csv


@dlt.table(
        name="standard",
        comment="Standard information from products provided from AWIN feed",
        table_properties={ "medalion": "bronze" }
)
def main(source_path: str, catalog: str, schema: str, table: str, remove_all_null: bool) -> None:
    raw_dataframe = load_raw_gzip_csv(source_path, remove_all_null)

