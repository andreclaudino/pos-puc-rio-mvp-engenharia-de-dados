"""
Funções para geração de queries SQL para dashboards.

Este módulo contém as funções que criam queries SQL
para visualização dos dados de sumarização estatística.
"""

from typing import List, Dict, Any

from .types import ColumnType


def create_dashboard_queries(catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Gera queries SQL para visualização do dashboard.
    
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
                        MAX(CASE WHEN metric_name = 'null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as null_count,
                        MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as non_null_count
                    FROM {summary_table}
                    WHERE metric_name IN ('null_count', 'non_null_count')
                    GROUP BY column_name, column_type
                )
                WHERE null_count > 0
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
                WHERE column_type = '{ColumnType.NUMERIC.value}' AND metric_name IN ('mean', 'median', 'min', 'max', 'q1', 'q3')
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
                WHERE column_type = '{ColumnType.NUMERIC.value}' AND metric_name IN ('p95', 'p99')
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
                WHERE column_type = '{ColumnType.CATEGORICAL.value}' AND metric_name = 'distinct_count'
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
                WHERE column_type = '{ColumnType.CATEGORICAL.value}' AND metric_name = 'value_counts'
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


def create_additional_queries(catalog: str, schema: str, table: str) -> List[str]:
    """
    Cria queries adicionais úteis para análise.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        
    Returns:
        Lista de strings com queries SQL
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    
    queries = [
        f"""
        -- Top 20 colunas com mais valores nulos:
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
                MAX(CASE WHEN metric_name = 'null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as null_count,
                MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as non_null_count
            FROM {summary_table}
            WHERE metric_name IN ('null_count', 'non_null_count')
            GROUP BY column_name, column_type
        )
        WHERE null_count > 0
        ORDER BY null_percentage DESC
        LIMIT 20;
        """,
        
        f"""
        -- Análise de distribuição para colunas numéricas específicas:
        -- Substitua 'nome_da_coluna' por uma coluna numérica real
        SELECT 
            metric_name,
            metric_value
        FROM {summary_table}
        WHERE column_name = 'nome_da_coluna' AND column_type = '{ColumnType.NUMERIC.value}'
        ORDER BY metric_name;
        """,
        
        f"""
        -- Comparação entre média e mediana por coluna numérica:
        SELECT 
            column_name,
            MAX(CASE WHEN metric_name = 'mean' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as media,
            MAX(CASE WHEN metric_name = 'median' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as mediana,
            ABS(
                MAX(CASE WHEN metric_name = 'mean' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) -
                MAX(CASE WHEN metric_name = 'median' THEN CAST(metric_value AS DOUBLE) ELSE NULL END)
            ) as diferenca_absoluta
        FROM {summary_table}
        WHERE column_type = '{ColumnType.NUMERIC.value}' AND metric_name IN ('mean', 'median')
        GROUP BY column_name
        ORDER BY diferenca_absoluta DESC;
        """,
        
        f"""
        -- Análise de cardinalidade por tipo de coluna:
        SELECT 
            column_type,
            COUNT(DISTINCT column_name) as total_colunas,
            AVG(CAST(metric_value AS DOUBLE)) as cardinalidade_media
        FROM {summary_table}
        WHERE metric_name = 'distinct_count'
        GROUP BY column_type
        ORDER BY total_colunas DESC;
        """
    ]
    
    return queries


def create_quality_assessment_query(catalog: str, schema: str, table: str) -> str:
    """
    Cria query para avaliação geral da qualidade dos dados.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        
    Returns:
        String com query SQL
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    
    return f"""
        -- Avaliação da Qualidade dos Dados
        WITH quality_metrics AS (
            SELECT 
                column_name,
                column_type,
                MAX(CASE WHEN metric_name = 'null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as null_count,
                MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as non_null_count,
                MAX(CASE WHEN metric_name = 'distinct_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as distinct_count
            FROM {summary_table}
            WHERE metric_name IN ('null_count', 'non_null_count', 'distinct_count')
            GROUP BY column_name, column_type
        ),
        quality_summary AS (
            SELECT 
                column_name,
                column_type,
                null_count,
                non_null_count,
                distinct_count,
                null_count + non_null_count as total_records,
                CAST(null_count AS DOUBLE) / (null_count + non_null_count) * 100 as null_percentage,
                CASE 
                    WHEN distinct_count = 1 THEN 'Baixa Cardinalidade'
                    WHEN distinct_count <= 10 THEN 'Média Cardinalidade'
                    WHEN distinct_count <= 100 THEN 'Alta Cardinalidade'
                    ELSE 'Muito Alta Cardinalidade'
                END as cardinalidade_level,
                CASE 
                    WHEN null_percentage > 50 THEN 'Pobre'
                    WHEN null_percentage > 20 THEN 'Regular'
                    WHEN null_percentage > 5 THEN 'Boa'
                    ELSE 'Excelente'
                END as qualidade_nulos
            FROM quality_metrics
        )
        SELECT 
            column_name,
            column_type,
            total_records,
            null_count,
            null_percentage,
            distinct_count,
            cardinalidade_level,
            qualidade_nulos,
            CASE 
                WHEN null_percentage < 5 AND distinct_count > 10 THEN 'Excelente'
                WHEN null_percentage < 20 AND distinct_count > 1 THEN 'Boa'
                WHEN null_percentage < 50 AND distinct_count >= 1 THEN 'Regular'
                ELSE 'Precisa Melhorias'
            END as avaliacao_geral
        FROM quality_summary
        ORDER BY column_name;
    """
