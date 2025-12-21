import click
import json
from typing import Dict, Any, List, Optional
import logging
from databricks.sdk.runtime import spark

from mvp.summarization.utils.dashboard_builder import create_interactive_dashboard

# Get logger for this module
logger = logging.getLogger(__name__)


@click.command()
@click.option("--catalog", type=click.STRING, envvar="CATALOG", required=True, help="The catalog name")
@click.option("--schema", type=click.STRING, envvar="SCHEMA", required=True, help="The schema name")
@click.option("--table", type=click.STRING, envvar="TABLE", required=True, help="The base table name (without _summary)")
@click.option("--dashboard-name", type=click.STRING, default=None, help="Custom dashboard name")
@click.option("--tags", type=click.STRING, multiple=True, default=None, help="Tags for the dashboard")
@click.option("--output-path", type=click.STRING, default="/tmp", help="Output directory for dashboard files")
@click.option("--dashboard-type", type=click.Choice(["interactive", "legacy"]), default="interactive", help="Type of dashboard to create")
def main(catalog: str, schema: str, table: str, dashboard_name: Optional[str], tags: Optional[List[str]], output_path: str, dashboard_type: str):
    """
    Create interactive dashboard configuration for table summary visualization.
    
    This function creates a modern, interactive dashboard with:
    - Big numbers for key metrics
    - Separate visualizations for numeric and categorical columns
    - Drill-down capabilities for detailed column analysis
    - Dynamic queries that update when data changes
    - Viridis color scheme compliance
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        dashboard_name: Nome personalizado para o dashboard
        tags: Lista de tags para o dashboard
        output_path: Diretório para salvar os arquivos gerados
        dashboard_type: Tipo de dashboard (interactive ou legacy)
    """
    try:
        logger.info(f"Creating {dashboard_type} dashboard configuration for {catalog}.{schema}.{table}")
        
        if dashboard_type == "interactive":
            return _create_interactive_dashboard(catalog, schema, table, dashboard_name, tags, output_path)
        else:
            return _create_legacy_dashboard(catalog, schema, table, output_path)
        
    except Exception as e:
        logger.error(f"Dashboard creation failed: {str(e)}")
        raise


def _create_interactive_dashboard(
    catalog: str, 
    schema: str, 
    table: str, 
    dashboard_name: Optional[str], 
    tags: Optional[List[str]], 
    output_path: str
) -> Dict[str, Any]:
    """
    Cria um dashboard interativo completo.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        dashboard_name: Nome personalizado para o dashboard
        tags: Lista de tags para o dashboard
        output_path: Diretório para salvar os arquivos gerados
        
    Returns:
        Dicionário com informações do dashboard criado
    """
    try:
        # Usar o novo construtor de dashboard interativo
        result = create_interactive_dashboard(
            catalog=catalog,
            schema=schema,
            table=table,
            dashboard_name=dashboard_name,
            tags=tags,
            output_path=output_path
        )
        
        # Exibir instruções de importação
        logger.info("=== INSTRUÇÕES DE IMPORTAÇÃO ===")
        for instruction in result["import_instructions"]:
            logger.info(instruction)
        
        logger.info(f"Interactive dashboard '{result['dashboard_name']}' created successfully!")
        logger.info(f"Files saved to: {output_path}")
        logger.info(f"Total widgets created: {result['widgets_count']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Interactive dashboard creation failed: {str(e)}")
        raise


def _create_legacy_dashboard(
    catalog: str, 
    schema: str, 
    table: str, 
    output_path: str
) -> Dict[str, Any]:
    """
    Cria um dashboard legado para compatibilidade.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        output_path: Diretório para salvar os arquivos gerados
        
    Returns:
        Dicionário com informações do dashboard criado
    """
    try:
        # Verificar se a tabela de sumário existe
        summary_table = f"{catalog}.{schema}_summary.{table}_summary"
        try:
            spark.sql(f"SELECT COUNT(*) FROM {summary_table}")
            logger.info(f"Summary table {summary_table} found")
        except Exception as e:
            logger.error(f"Summary table {summary_table} not found: {str(e)}")
            raise
        
        # Gerar configuração JSON do dashboard legado
        dashboard_json = _create_legacy_dashboard_json(catalog, schema, table)
        json_path = f"{output_path}/dashboard_{table}.json"
        
        # Gerar script SQL
        sql_script = _generate_legacy_sql_script(catalog, schema, table)
        sql_path = f"{output_path}/dashboard_{table}.sql"
        
        # Salvar arquivos usando dbutils
        try:
            dbutils.fs.put(f"file:{json_path}", json.dumps(dashboard_json, indent=2, ensure_ascii=False), overwrite=True)
            logger.info(f"Legacy dashboard JSON saved to: {json_path}")
        except:
            with open(json_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps(dashboard_json, indent=2, ensure_ascii=False))
            logger.info(f"Legacy dashboard JSON saved locally to: {json_path}")
        
        try:
            dbutils.fs.put(f"file:{sql_path}", sql_script, overwrite=True)
            logger.info(f"Legacy dashboard SQL saved to: {sql_path}")
        except:
            with open(sql_path, 'w', encoding='utf-8') as f:
                f.write(sql_script)
            logger.info(f"Legacy dashboard SQL saved locally to: {sql_path}")
        
        # Exibir queries de exemplo
        logger.info("Sample queries for legacy dashboard:")
        queries = _create_legacy_dashboard_queries(catalog, schema, table)
        for query in queries[:3]:  # Show first 3 queries
            logger.info(f"  - {query['title']}")
        
        logger.info("Legacy dashboard configuration created successfully!")
        logger.info("You can use the SQL script to create visualizations in Databricks SQL Editor")
        
        return {
            "dashboard_name": f"Sumarização - {table}",
            "json_path": json_path,
            "sql_path": sql_path,
            "widgets_count": len(queries),
            "dashboard_type": "legacy"
        }
        
    except Exception as e:
        logger.error(f"Legacy dashboard creation failed: {str(e)}")
        raise


def _create_legacy_dashboard_queries(catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Gera queries SQL para dashboard legado (mantido para compatibilidade).
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base (sem _summary)
        
    Returns:
        Lista de dicionários com configurações de queries
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    
    queries = [
        {
            "title": "Visão Geral das Colunas",
            "description": "Resumo geral dos tipos de colunas e contagens",
            "query": f"""
                SELECT 
                    column_name,
                    column_type,
                    MAX(CASE WHEN metric_name = 'distinct_count' THEN metric_value ELSE NULL END) as distinct_count,
                    MAX(CASE WHEN metric_name = 'null_count' THEN metric_value ELSE NULL END) as null_count,
                    MAX(CASE WHEN metric_name = 'non_null_count' THEN metric_value ELSE NULL END) as non_null_count
                FROM {summary_table}
                WHERE metric_name IN ('distinct_count', 'null_count', 'non_null_count')
                GROUP BY column_name, column_type
                ORDER BY column_name
            """,
            "chart_type": "table"
        },
        {
            "title": "Distribuição de Valores Nulos",
            "description": "Percentual de valores nulos por coluna",
            "query": f"""
                SELECT 
                    column_name,
                    column_type,
                    CAST(null_count AS DOUBLE) / (CAST(null_count AS DOUBLE) + CAST(non_null_count AS DOUBLE)) * 100 as null_percentage
                FROM (
                    SELECT 
                        column_name,
                        column_type,
                        MAX(CASE WHEN metric_name = 'null_count' THEN metric_value ELSE NULL END) as null_count,
                        MAX(CASE WHEN metric_name = 'non_null_count' THEN metric_value ELSE NULL END) as non_null_count
                    FROM {summary_table}
                    WHERE metric_name IN ('null_count', 'non_null_count')
                    GROUP BY column_name, column_type
                )
                ORDER BY null_percentage DESC
            """,
            "chart_type": "bar"
        },
        {
            "title": "Estatísticas Descritivas - Colunas Numéricas",
            "description": "Média, mediana e quartis para colunas numéricas",
            "query": f"""
                SELECT 
                    column_name,
                    MAX(CASE WHEN metric_name = 'mean' THEN metric_value ELSE NULL END) as media,
                    MAX(CASE WHEN metric_name = 'median' THEN metric_value ELSE NULL END) as mediana,
                    MAX(CASE WHEN metric_name = 'min' THEN metric_value ELSE NULL END) as minimo,
                    MAX(CASE WHEN metric_name = 'max' THEN metric_value ELSE NULL END) as maximo,
                    MAX(CASE WHEN metric_name = 'q1' THEN metric_value ELSE NULL END) as q1,
                    MAX(CASE WHEN metric_name = 'q3' THEN metric_value ELSE NULL END) as q3
                FROM {summary_table}
                WHERE column_type = 'numeric' AND metric_name IN ('mean', 'median', 'min', 'max', 'q1', 'q3')
                GROUP BY column_name
                ORDER BY column_name
            """,
            "chart_type": "table"
        },
        {
            "title": "Percentis - Colunas Numéricas",
            "description": "Comparação entre percentis 95 e 99",
            "query": f"""
                SELECT 
                    column_name,
                    MAX(CASE WHEN metric_name = 'p95' THEN metric_value ELSE NULL END) as p95,
                    MAX(CASE WHEN metric_name = 'p99' THEN metric_value ELSE NULL END) as p99
                FROM {summary_table}
                WHERE column_type = 'numeric' AND metric_name IN ('p95', 'p99')
                GROUP BY column_name
                ORDER BY column_name
            """,
            "chart_type": "bar"
        },
        {
            "title": "Análise de Cardinalidade - Colunas Categóricas",
            "description": "Número de valores distintos por coluna categórica",
            "query": f"""
                SELECT 
                    column_name,
                    CAST(metric_value AS INTEGER) as distinct_values
                FROM {summary_table}
                WHERE column_type = 'categorical' AND metric_name = 'distinct_count'
                ORDER BY distinct_values DESC
            """,
            "chart_type": "bar"
        },
        {
            "title": "Top 10 Valores Mais Frequentes por Coluna",
            "description": "Análise dos valores mais comuns em colunas categóricas",
            "query": f"""
                SELECT 
                    column_name,
                    metric_name,
                    metric_value
                FROM {summary_table}
                WHERE column_type = 'categorical' AND metric_name = 'value_counts'
                LIMIT 10
            """,
            "chart_type": "table"
        },
        {
            "title": "Resumo de Métricas por Tipo de Coluna",
            "description": "Contagem de colunas por tipo e métricas disponíveis",
            "query": f"""
                SELECT 
                    column_type,
                    metric_name,
                    COUNT(*) as count
                FROM {summary_table}
                GROUP BY column_type, metric_name
                ORDER BY column_type, metric_name
            """,
            "chart_type": "pie"
        }
    ]
    
    return queries


def _create_legacy_dashboard_json(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """
    Cria configuração de dashboard em formato JSON (legado).
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema
        table: Nome da tabela base
        
    Returns:
        Dicionário com configuração do dashboard
    """
    dashboard_name = f"Sumarização - {table}"
    queries = _create_legacy_dashboard_queries(catalog, schema, table)
    
    dashboard_config = {
        "name": dashboard_name,
        "description": f"Dashboard de análise estatística da tabela {table}",
        "tags": ["sumarização", "estatística", table],
        "widgets": []
    }
    
    for i, query_config in enumerate(queries):
        widget = {
            "widget": {
                "title": query_config["title"],
                "description": query_config["description"],
                "visualization": {
                    "type": query_config["chart_type"],
                    "query": query_config["query"]
                },
                "position": {
                    "x": (i % 2) * 600,
                    "y": (i // 2) * 400,
                    "width": 550,
                    "height": 350
                }
            }
        }
        dashboard_config["widgets"].append(widget)
    
    return dashboard_config


def _generate_legacy_sql_script(catalog: str, schema: str, table: str) -> str:
    """
    Gera script SQL para criação manual de dashboard (legado).
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema
        table: Nome da tabela base
        
    Returns:
        Script SQL como string
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    queries = _create_legacy_dashboard_queries(catalog, schema, table)
    
    sql_script = f"-- Dashboard SQL Script para {catalog}.{schema}.{table}\n"
    sql_script += f"-- Tabela de sumário: {summary_table}\n\n"
    
    for i, query_config in enumerate(queries):
        sql_script += f"-- {query_config['title']}\n"
        sql_script += f"-- {query_config['description']}\n"
        sql_script += f"-- Chart Type: {query_config['chart_type']}\n"
        sql_script += f"{query_config['query']};\n\n"
    
    # Adicionar queries úteis adicionais
    sql_script += "-- Consultas Adicionais Úteis\n\n"
    
    sql_script += "-- Top 20 colunas com mais valores nulos:\n"
    sql_script += f"""
    SELECT 
        column_name,
        column_type,
        CAST(null_count AS DOUBLE) / (CAST(null_count AS DOUBLE) + CAST(non_null_count AS DOUBLE)) * 100 as null_percentage,
        null_count,
        non_null_count
    FROM (
        SELECT 
            column_name,
            column_type,
            MAX(CASE WHEN metric_name = 'null_count' THEN metric_value ELSE NULL END) as null_count,
            MAX(CASE WHEN metric_name = 'non_null_count' THEN metric_value ELSE NULL END) as non_null_count
        FROM {summary_table}
        WHERE metric_name IN ('null_count', 'non_null_count')
        GROUP BY column_name, column_type
    )
    WHERE null_count > 0
    ORDER BY null_percentage DESC
    LIMIT 20;
    """
    
    sql_script += "\n\n-- Análise de distribuição para colunas numéricas específicas:\n"
    sql_script += f"""
    -- Substitua 'nome_da_coluna' por uma coluna numérica real
    SELECT 
        metric_name,
        metric_value
    FROM {summary_table}
    WHERE column_name = 'nome_da_coluna' AND column_type = 'numeric'
    ORDER BY metric_name;
    """
    
    return sql_script


def run_main():
    """
    Wrapper function for main with error handling.
    """
    try:
        # standalone_mode=False prevents click from calling sys.exit()
        main(standalone_mode=False)
    except Exception as e:
        # Ensure real errors still fail the job with a clear message
        print(f"Job failed with error: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    run_main()
