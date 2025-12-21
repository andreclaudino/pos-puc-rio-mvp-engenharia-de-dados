"""
Tipos e constantes para o módulo de sumarização.

Este módulo define tipos de dados e constantes utilizadas
nas análises estatísticas de tabelas.
"""

from typing import Dict, Any, List
from enum import Enum


class ColumnType(Enum):
    """Enum para tipos de colunas suportados."""
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"


class MetricType(Enum):
    """Enum para tipos de métricas calculadas."""
    # Métricas numéricas
    MEAN = "mean"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    Q1 = "q1"
    Q3 = "q3"
    P95 = "p95"
    P99 = "p99"
    
    # Métricas categóricas
    DISTINCT_COUNT = "distinct_count"
    DISTINCT_VALUES_SORTED = "distinct_values_sorted"
    VALUE_COUNTS = "value_counts"
    
    # Métricas comuns
    NULL_COUNT = "null_count"
    NON_NULL_COUNT = "non_null_count"


# Mapeamento de métricas para descrições
METRIC_DESCRIPTIONS: Dict[str, str] = {
    MetricType.MEAN.value: "Valor médio da coluna",
    MetricType.MEDIAN.value: "Valor mediano (50º percentil)",
    MetricType.MIN.value: "Valor mínimo encontrado",
    MetricType.MAX.value: "Valor máximo encontrado",
    MetricType.Q1.value: "Primeiro quartil (25º percentil)",
    MetricType.Q3.value: "Terceiro quartil (75º percentil)",
    MetricType.P95.value: "Percentil 95",
    MetricType.P99.value: "Percentil 99",
    MetricType.DISTINCT_COUNT.value: "Total de valores distintos",
    MetricType.DISTINCT_VALUES_SORTED.value: "Lista de valores distintos ordenados",
    MetricType.VALUE_COUNTS.value: "Contagem de ocorrências por valor",
    MetricType.NULL_COUNT.value: "Total de valores nulos",
    MetricType.NON_NULL_COUNT.value: "Total de valores não nulos"
}


# Métricas por tipo de coluna
NUMERIC_METRICS = [
    MetricType.MEAN.value,
    MetricType.MEDIAN.value,
    MetricType.MIN.value,
    MetricType.MAX.value,
    MetricType.Q1.value,
    MetricType.Q3.value,
    MetricType.P95.value,
    MetricType.P99.value,
    MetricType.NULL_COUNT.value,
    MetricType.NON_NULL_COUNT.value
]

CATEGORICAL_METRICS = [
    MetricType.DISTINCT_COUNT.value,
    MetricType.DISTINCT_VALUES_SORTED.value,
    MetricType.VALUE_COUNTS.value,
    MetricType.NULL_COUNT.value,
    MetricType.NON_NULL_COUNT.value
]


# Tipos de dados Spark considerados numéricos
NUMERIC_SPARK_TYPES = [
    "integer",
    "long", 
    "double",
    "decimal",
    "float"
]


# Schema da tabela de sumário
SUMMARY_TABLE_SCHEMA = {
    "column_name": "VARCHAR(255)",
    "column_type": "VARCHAR(50)",
    "metric_name": "VARCHAR(100)",
    "metric_value": "TEXT",
    "metric_description": "VARCHAR(500)"
}


def get_metric_description(metric_name: str) -> str:
    """
    Retorna a descrição de uma métrica.
    
    Args:
        metric_name: Nome da métrica
        
    Returns:
        Descrição da métrica
    """
    return METRIC_DESCRIPTIONS.get(metric_name, "Métrica desconhecida")


def is_numeric_column(spark_type: str) -> bool:
    """
    Verifica se um tipo do Spark é considerado numérico.
    
    Args:
        spark_type: Tipo de dado do Spark como string
        
    Returns:
        True se for numérico, False caso contrário
    """
    return any(num_type in spark_type.lower() for num_type in NUMERIC_SPARK_TYPES)


def get_column_type_metrics(column_type: str) -> List[str]:
    """
    Retorna a lista de métricas para um tipo de coluna.
    
    Args:
        column_type: Tipo da coluna (numeric/categorical)
        
    Returns:
        Lista de métricas aplicáveis
    """
    if column_type == ColumnType.NUMERIC.value:
        return NUMERIC_METRICS
    elif column_type == ColumnType.CATEGORICAL.value:
        return CATEGORICAL_METRICS
    else:
        return []


def generate_summary_schema_info(table_name: str) -> Dict[str, Any]:
    """
    Gera metadados do schema para a tabela de sumário.
    
    Args:
        table_name: Nome da tabela original
        
    Returns:
        Dicionário com metadados da tabela e colunas
    """
    return {
        "table_description": f"Tabela de sumarização estatística da tabela {table_name}",
        "columns": {
            "column_name": {
                "data_type": "VARCHAR(255)",
                "description": "Nome da coluna analisada na tabela original"
            },
            "column_type": {
                "data_type": "VARCHAR(50)",
                "description": "Tipo da coluna (numeric ou categorical)"
            },
            "metric_name": {
                "data_type": "VARCHAR(100)",
                "description": "Nome da métrica estatística calculada"
            },
            "metric_value": {
                "data_type": "TEXT",
                "description": "Valor da métrica calculada"
            },
            "metric_description": {
                "data_type": "VARCHAR(500)",
                "description": "Descrição do que representa a métrica"
            }
        }
    }
