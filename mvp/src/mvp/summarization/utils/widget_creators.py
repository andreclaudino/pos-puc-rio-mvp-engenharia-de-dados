"""
Funções para criação de widgets de dashboard Databricks.

Este módulo contém funções especializadas para criar diferentes
tipos de widgets para dashboards interativos.
"""

from typing import Dict, Any, List, Optional
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import dashboards

from .types import ColumnType

# Get logger for this module
logger = logging.getLogger(__name__)

# Paleta de cores viridis conforme diretrizes
VIRIDIS_COLORS = [
    "#fde725", "#eae51a", "#d2e21b", "#bade28", "#a2da37",
    "#8bd646", "#77d153", "#63cb5f", "#50c46a", "#3fbc73",
    "#31b57b", "#26ad81", "#21a585", "#1e9d89", "#1f948c",
    "#228c8d", "#25838e", "#297b8e", "#2c738e", "#2f6b8e",
    "#33628d", "#38598c", "#3c4f8a", "#404588", "#443b84",
    "#46307e", "#482576", "#481a6c", "#470d60", "#440154"
]


def create_bignumber_widgets(catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Cria widgets de big numbers para o topo do dashboard.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        
    Returns:
        Lista de configurações de widgets big number
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    original_table = f"{catalog}.{schema}.{table}"
    
    widgets = [
        {
            "title": "Total de Linhas",
            "description": "Número total de registros na tabela original",
            "visualization": {
                "type": "big_number",
                "query": f"""
                    SELECT 
                        MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS LONG) ELSE 0 END) as total_rows
                    FROM {summary_table}
                    WHERE column_name = (
                        SELECT column_name 
                        FROM {summary_table} 
                        WHERE metric_name = 'non_null_count'
                        ORDER BY CAST(metric_value AS LONG) DESC 
                        LIMIT 1
                    )
                    AND metric_name = 'non_null_count'
                """,
                "format": {
                    "prefix": "",
                    "suffix": " registros",
                    "decimal_places": 0
                }
            },
            "position": {"x": 0, "y": 0, "width": 240, "height": 120},
            "color": VIRIDIS_COLORS[0]
        },
        {
            "title": "Total de Colunas",
            "description": "Número total de colunas na tabela",
            "visualization": {
                "type": "big_number",
                "query": f"""
                    SELECT COUNT(DISTINCT column_name) as total_columns
                    FROM {summary_table}
                """,
                "format": {
                    "prefix": "",
                    "suffix": " colunas",
                    "decimal_places": 0
                }
            },
            "position": {"x": 250, "y": 0, "width": 240, "height": 120},
            "color": VIRIDIS_COLORS[5]
        },
        {
            "title": "Colunas Numéricas",
            "description": "Número de colunas do tipo numérico",
            "visualization": {
                "type": "big_number",
                "query": f"""
                    SELECT COUNT(DISTINCT column_name) as numeric_columns
                    FROM {summary_table}
                    WHERE column_type = '{ColumnType.NUMERIC.value}'
                """,
                "format": {
                    "prefix": "",
                    "suffix": " colunas",
                    "decimal_places": 0
                }
            },
            "position": {"x": 500, "y": 0, "width": 240, "height": 120},
            "color": VIRIDIS_COLORS[10]
        },
        {
            "title": "Colunas Categóricas",
            "description": "Número de colunas do tipo categórico",
            "visualization": {
                "type": "big_number",
                "query": f"""
                    SELECT COUNT(DISTINCT column_name) as categorical_columns
                    FROM {summary_table}
                    WHERE column_type = '{ColumnType.CATEGORICAL.value}'
                """,
                "format": {
                    "prefix": "",
                    "suffix": " colunas",
                    "decimal_places": 0
                }
            },
            "position": {"x": 750, "y": 0, "width": 240, "height": 120},
            "color": VIRIDIS_COLORS[15]
        },
        {
            "title": "Percentual de Nulos",
            "description": "Percentual médio de valores nulos na tabela",
            "visualization": {
                "type": "big_number",
                "query": f"""
                    SELECT 
                        AVG(
                            CAST(null_count AS DOUBLE) / 
                            (CAST(null_count AS DOUBLE) + CAST(non_null_count AS DOUBLE)) * 100
                        ) as avg_null_percentage
                    FROM (
                        SELECT 
                            column_name,
                            MAX(CASE WHEN metric_name = 'null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as null_count,
                            MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as non_null_count
                        FROM {summary_table}
                        WHERE metric_name IN ('null_count', 'non_null_count')
                        GROUP BY column_name
                    )
                """,
                "format": {
                    "prefix": "",
                    "suffix": "% nulos",
                    "decimal_places": 2
                }
            },
            "position": {"x": 1000, "y": 0, "width": 240, "height": 120},
            "color": VIRIDIS_COLORS[20]
        }
    ]
    
    return widgets


def create_filter_widgets() -> List[Dict[str, Any]]:
    """
    Cria widgets de filtro para interatividade.
    
    Returns:
        Lista de configurações de widgets de filtro
    """
    widgets = [
        {
            "title": "Tipo de Coluna",
            "description": "Filtre por tipo de coluna",
            "visualization": {
                "type": "dropdown",
                "query": """
                    SELECT DISTINCT column_type as value, column_type as label
                    FROM (
                        SELECT 'Todas' as column_type
                        UNION ALL
                        SELECT 'numeric' as column_type
                        UNION ALL  
                        SELECT 'categorical' as column_type
                    )
                    ORDER BY column_type
                """,
                "parameter_name": "column_type_filter"
            },
            "position": {"x": 0, "y": 130, "width": 200, "height": 80}
        }
    ]
    
    return widgets


def create_numeric_visualizations(catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Cria visualizações específicas para colunas numéricas.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        
    Returns:
        Lista de configurações de widgets para colunas numéricas
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    
    widgets = [
        {
            "title": "Estatísticas Descritivas - Numéricas",
            "description": "Média, mediana e quartis para colunas numéricas",
            "visualization": {
                "type": "table",
                "query": f"""
                    SELECT 
                        column_name,
                        MAX(CASE WHEN metric_name = 'mean' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as média,
                        MAX(CASE WHEN metric_name = 'median' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as mediana,
                        MAX(CASE WHEN metric_name = 'min' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as mínimo,
                        MAX(CASE WHEN metric_name = 'max' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as máximo,
                        MAX(CASE WHEN metric_name = 'q1' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as q1,
                        MAX(CASE WHEN metric_name = 'q3' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as q3
                    FROM {summary_table}
                    WHERE column_type = '{ColumnType.NUMERIC.value}' 
                        AND metric_name IN ('mean', 'median', 'min', 'max', 'q1', 'q3')
                    GROUP BY column_name
                    ORDER BY column_name
                """,
                "column_settings": {
                    "média": {"format": {"decimal_places": 2}},
                    "mediana": {"format": {"decimal_places": 2}},
                    "mínimo": {"format": {"decimal_places": 2}},
                    "máximo": {"format": {"decimal_places": 2}},
                    "q1": {"format": {"decimal_places": 2}},
                    "q3": {"format": {"decimal_places": 2}}
                }
            },
            "position": {"x": 0, "y": 220, "width": 580, "height": 300},
            "color_scheme": VIRIDIS_COLORS[:6]
        },
        {
            "title": "Comparação de Percentis - Numéricas",
            "description": "Comparação entre percentis 95 e 99 para colunas numéricas",
            "visualization": {
                "type": "bar",
                "query": f"""
                    SELECT 
                        column_name,
                        MAX(CASE WHEN metric_name = 'p95' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as p95,
                        MAX(CASE WHEN metric_name = 'p99' THEN CAST(metric_value AS DOUBLE) ELSE NULL END) as p99
                    FROM {summary_table}
                    WHERE column_type = '{ColumnType.NUMERIC.value}' 
                        AND metric_name IN ('p95', 'p99')
                    GROUP BY column_name
                    ORDER BY column_name
                    LIMIT 15
                """,
                "x_axis": "column_name",
                "y_axis": ["p95", "p99"],
                "stacking": "grouped"
            },
            "position": {"x": 590, "y": 220, "width": 580, "height": 300},
            "color_scheme": [VIRIDIS_COLORS[8], VIRIDIS_COLORS[12]]
        },
        {
            "title": "Distribuição de Nulos - Numéricas",
            "description": "Percentual de valores nulos por coluna numérica",
            "visualization": {
                "type": "bar",
                "query": f"""
                    SELECT 
                        column_name,
                        CAST(null_count AS DOUBLE) / (CAST(null_count AS DOUBLE) + CAST(non_null_count AS DOUBLE)) * 100 as null_percentage
                    FROM (
                        SELECT 
                            column_name,
                            MAX(CASE WHEN metric_name = 'null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as null_count,
                            MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as non_null_count
                        FROM {summary_table}
                        WHERE column_type = '{ColumnType.NUMERIC.value}' 
                            AND metric_name IN ('null_count', 'non_null_count')
                        GROUP BY column_name
                    )
                    WHERE null_count > 0
                    ORDER BY null_percentage DESC
                    LIMIT 10
                """,
                "x_axis": "column_name",
                "y_axis": "null_percentage",
                "format": {"y_axis": {"suffix": "%", "decimal_places": 1}}
            },
            "position": {"x": 1180, "y": 220, "width": 400, "height": 300},
            "color": VIRIDIS_COLORS[18]
        }
    ]
    
    return widgets


def create_categorical_visualizations(catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Cria visualizações específicas para colunas categóricas.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        
    Returns:
        Lista de configurações de widgets para colunas categóricas
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    
    widgets = [
        {
            "title": "Cardinalidade - Categóricas",
            "description": "Número de valores distintos por coluna categórica",
            "visualization": {
                "type": "bar",
                "query": f"""
                    SELECT 
                        column_name,
                        CAST(metric_value AS INTEGER) as distinct_values
                    FROM {summary_table}
                    WHERE column_type = '{ColumnType.CATEGORICAL.value}' 
                        AND metric_name = 'distinct_count'
                    ORDER BY distinct_values DESC
                    LIMIT 15
                """,
                "x_axis": "column_name",
                "y_axis": "distinct_values",
                "format": {"y_axis": {"decimal_places": 0}}
            },
            "position": {"x": 0, "y": 530, "width": 580, "height": 300},
            "color": VIRIDIS_COLORS[7]
        },
        {
            "title": "Qualidade dos Dados - Categóricas",
            "description": "Análise de completude para colunas categóricas",
            "visualization": {
                "type": "table",
                "query": f"""
                    SELECT 
                        column_name,
                        CAST(metric_value AS INTEGER) as total_distinct,
                        CASE 
                            WHEN CAST(metric_value AS INTEGER) = 1 THEN 'Muito Baixa'
                            WHEN CAST(metric_value AS INTEGER) <= 5 THEN 'Baixa'
                            WHEN CAST(metric_value AS INTEGER) <= 20 THEN 'Média'
                            WHEN CAST(metric_value AS INTEGER) <= 100 THEN 'Alta'
                            ELSE 'Muito Alta'
                        END as cardinalidade,
                        null_percentage
                    FROM (
                        SELECT 
                            s.column_name,
                            s.metric_value,
                            n.null_percentage
                        FROM {summary_table} s
                        JOIN (
                            SELECT 
                                column_name,
                                CAST(null_count AS DOUBLE) / (CAST(null_count AS DOUBLE) + CAST(non_null_count AS DOUBLE)) * 100 as null_percentage
                            FROM (
                                SELECT 
                                    column_name,
                                    MAX(CASE WHEN metric_name = 'null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as null_count,
                                    MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as non_null_count
                                FROM {summary_table}
                                WHERE metric_name IN ('null_count', 'non_null_count')
                                GROUP BY column_name
                            )
                        ) n ON s.column_name = n.column_name
                        WHERE s.column_type = '{ColumnType.CATEGORICAL.value}' 
                            AND s.metric_name = 'distinct_count'
                    )
                    ORDER BY total_distinct DESC
                """,
                "column_settings": {
                    "total_distinct": {"format": {"decimal_places": 0}},
                    "null_percentage": {"format": {"suffix": "%", "decimal_places": 1}}
                }
            },
            "position": {"x": 590, "y": 530, "width": 580, "height": 300},
            "color_scheme": VIRIDIS_COLORS[10:15]
        },
        {
            "title": "Top Valores - Categóricas",
            "description": "Valores mais frequentes em colunas categóricas (amostra)",
            "visualization": {
                "type": "table",
                "query": f"""
                    SELECT 
                        column_name,
                        metric_name,
                        metric_value
                    FROM {summary_table}
                    WHERE column_type = '{ColumnType.CATEGORICAL.value}' 
                        AND metric_name = 'value_counts'
                    ORDER BY column_name, metric_name
                    LIMIT 20
                """,
                "column_settings": {
                    "metric_value": {"format": {"decimal_places": 0}}
                }
            },
            "position": {"x": 1180, "y": 530, "width": 400, "height": 300},
            "color_scheme": VIRIDIS_COLORS[15:20]
        }
    ]
    
    return widgets


def create_drill_down_widgets(catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Cria visualizações de drill-down para análise detalhada de colunas.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        
    Returns:
        Lista de configurações de widgets para drill-down
    """
    summary_table = f"{catalog}.{schema}_summary.{table}_summary"
    
    widgets = [
        {
            "title": "Detalhes da Coluna Selecionada",
            "description": "Análise detalhada da coluna (selecione nos gráficos acima)",
            "visualization": {
                "type": "table",
                "query": f"""
                    SELECT 
                        metric_name,
                        metric_value,
                        CASE metric_name
                            WHEN 'mean' THEN 'Média'
                            WHEN 'median' THEN 'Mediana'
                            WHEN 'min' THEN 'Mínimo'
                            WHEN 'max' THEN 'Máximo'
                            WHEN 'q1' THEN '1º Quartil'
                            WHEN 'q3' THEN '3º Quartil'
                            WHEN 'p95' THEN 'Percentil 95'
                            WHEN 'p99' THEN 'Percentil 99'
                            WHEN 'distinct_count' THEN 'Valores Distintos'
                            WHEN 'null_count' THEN 'Valores Nulos'
                            WHEN 'non_null_count' THEN 'Valores Não Nulos'
                            ELSE metric_name
                        END as descricao
                    FROM {summary_table}
                    WHERE column_name = '{{selected_column}}'
                    ORDER BY metric_name
                """,
                "column_settings": {
                    "metric_value": {"format": {"decimal_places": 2}}
                }
            },
            "position": {"x": 0, "y": 840, "width": 580, "height": 250},
            "color_scheme": VIRIDIS_COLORS[5:10]
        },
        {
            "title": "Comparação de Métricas - Coluna Selecionada",
            "description": "Comparação visual das métricas da coluna selecionada",
            "visualization": {
                "type": "bar",
                "query": f"""
                    SELECT 
                        CASE metric_name
                            WHEN 'mean' THEN 'Média'
                            WHEN 'median' THEN 'Mediana'
                            WHEN 'min' THEN 'Mínimo'
                            WHEN 'max' THEN 'Máximo'
                            WHEN 'q1' THEN 'Q1'
                            WHEN 'q3' THEN 'Q3'
                            WHEN 'p95' THEN 'P95'
                            WHEN 'p99' THEN 'P99'
                            ELSE metric_name
                        END as metric_label,
                        CAST(metric_value AS DOUBLE) as value
                    FROM {summary_table}
                    WHERE column_name = '{{selected_column}}'
                        AND metric_name IN ('mean', 'median', 'min', 'max', 'q1', 'q3', 'p95', 'p99')
                    ORDER BY 
                        CASE metric_name
                            WHEN 'min' THEN 1
                            WHEN 'q1' THEN 2
                            WHEN 'median' THEN 3
                            WHEN 'mean' THEN 4
                            WHEN 'q3' THEN 5
                            WHEN 'p95' THEN 6
                            WHEN 'p99' THEN 7
                            WHEN 'max' THEN 8
                            ELSE 9
                        END
                """,
                "x_axis": "metric_label",
                "y_axis": "value"
            },
            "position": {"x": 590, "y": 840, "width": 580, "height": 250},
            "color": VIRIDIS_COLORS[12]
        }
    ]
    
    return widgets
