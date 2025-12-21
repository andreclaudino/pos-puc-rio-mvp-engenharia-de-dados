import click
import logging
from typing import Dict, Any

from databricks.sdk.runtime import spark

from shared.persistence import save_dataframe_with_metadata
from .utils.analyzers import identify_column_types, analyze_numeric_column, analyze_categorical_column, create_summary_dataframe
from .utils.types import generate_summary_schema_info


# Get logger for this module
logger = logging.getLogger(__name__)


def _load_table_from_catalog(catalog: str, schema: str, table: str):
    """
    Carrega uma tabela do Databricks Catalog.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema
        table: Nome da tabela
        
    Returns:
        Spark DataFrame
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    try:
        df = spark.table(full_table_name)
        logger.info(f"Tabela carregada com sucesso: {full_table_name}")
        return df
    except Exception as e:
        logger.error(f"Falha ao carregar tabela {full_table_name}: {str(e)}")
        raise


@click.command()
@click.option("--catalog", type=click.STRING, envvar="CATALOG", required=True, help="The source catalog name")
@click.option("--schema", type=click.STRING, envvar="SCHEMA", required=True, help="The source schema name")
@click.option("--table", type=click.STRING, envvar="TABLE", required=True, help="The source table name")
@click.option("--write-mode", type=click.Choice(["overwrite", "append"]), envvar="WRITE_MODE", default="overwrite", help="Spark write mode")
def main(catalog: str, schema: str, table: str, write_mode: str):
    """
    Gera sumário estatístico para uma tabela do Databricks e salva em tabela de sumário.
    """
    try:
        logger.info(f"Iniciando sumarização da tabela {catalog}.{schema}.{table}")
        
        # Carregar tabela de origem
        source_df = _load_table_from_catalog(catalog, schema, table)
        
        # Identificar tipos de colunas
        column_types = identify_column_types(source_df)
        logger.info(f"Encontradas {len(column_types['numeric'])} colunas numéricas e {len(column_types['categorical'])} colunas categóricas")
        
        # Analisar todas as colunas
        all_metrics = []
        
        # Analisar colunas numéricas
        for column in column_types['numeric']:
            logger.info(f"Analisando coluna numérica: {column}")
            metrics = analyze_numeric_column(source_df, column)
            all_metrics.extend(metrics)
        
        # Analisar colunas categóricas
        for column in column_types['categorical']:
            logger.info(f"Analisando coluna categórica: {column}")
            metrics = analyze_categorical_column(source_df, column)
            all_metrics.extend(metrics)
        
        # Criar DataFrame de sumário
        summary_df = create_summary_dataframe(all_metrics)
        
        # Preparar nomes de saída
        output_table = f"{table}_summary"
        output_schema = f"{schema}_summary"
        
        # Gerar metadados do schema
        schema_info = generate_summary_schema_info(table)
        
        # Salvar tabela de sumário com metadados
        save_dataframe_with_metadata(summary_df, catalog, output_schema, output_table, write_mode, schema_info)
        
        logger.info(f"Tabela de sumário criada com sucesso: {catalog}.{output_schema}.{output_table}")
        logger.info(f"Gerados {len(all_metrics)} entradas de métricas para {len(source_df.columns)} colunas")
        
        return True
        
    except Exception as e:
        logger.error(f"Falha na sumarização da tabela: {str(e)}")
        raise


def run_main():
    """
    Função wrapper para main com tratamento de erros.
    """
    try:
        # standalone_mode=False impede que o click chame sys.exit()
        main(standalone_mode=False)
    except Exception as e:
        # Garante que erros reais ainda falhem o job com mensagem clara
        print(f"Job failed with error: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    run_main()
