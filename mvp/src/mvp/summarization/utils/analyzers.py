"""
Funções de análise estatística para colunas de DataFrames Spark.

Este módulo contém as funções especializadas em calcular
estatísticas para diferentes tipos de colunas.
"""

import json
import logging
from typing import Dict, Any, List, Optional

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from databricks.sdk.runtime import spark

from .types import (
    ColumnType, MetricType, get_metric_description, 
    is_numeric_column, get_column_type_metrics
)


# Get logger for this module
logger = logging.getLogger(__name__)


def _validate_numeric_content(df, column_name: str) -> bool:
    """
    Valida se o conteúdo de uma coluna é realmente numérico.
    
    Args:
        df: Spark DataFrame
        column_name: Nome da coluna para validar
        
    Returns:
        True se os dados forem realmente numéricos, False caso contrário
    """
    try:
        # Tentar converter amostra para float
        sample_df = df.select(column_name).limit(100)
        
        # Tentar converter para double
        converted_df = sample_df.withColumn(column_name, F.col(column_name).cast("double"))
        
        # Comparar contagem de valores não nulos antes e depois da conversão
        original_non_null = sample_df.filter(F.col(column_name).isNotNull()).count()
        converted_non_null = converted_df.filter(F.col(column_name).isNotNull()).count()
        
        # Se a conversão manteve os mesmos valores, provavelmente são numéricos
        if original_non_null == converted_non_null:
            logger.info(f"Campo '{column_name}' validado como contendo dados numéricos reais")
            return True
        
        # Calcular razão entre conversões bem-sucedidas vs mal-sucedidas
        total_rows = sample_df.count()
        successful_conversions = converted_df.filter(F.col(column_name).isNotNull()).count()
        conversion_ratio = successful_conversions / total_rows if total_rows > 0 else 0
        
        # Se mais de 80% das conversões foram bem-sucedidas, considerar como numérico
        is_numeric = conversion_ratio >= 0.8
        
        if is_numeric:
            logger.info(f"Campo '{column_name}' validado como numérico (taxa de conversão: {conversion_ratio:.2%})")
        else:
            logger.warning(f"Campo '{column_name}' marcado como numérico mas contém dados não numéricos. Taxa de conversão: {conversion_ratio:.2%}")
        
        return is_numeric
        
    except Exception as e:
        logger.warning(f"Erro ao validar conteúdo numérico da coluna '{column_name}': {str(e)}")
        # Em caso de erro, assumir que não é numérico para evitar tratamento incorreto
        return False


def _detect_column_type_heuristic(field_name: str, df, json_type: Optional[str] = None) -> str:
    """
    Detecção heurística do tipo de coluna baseada em múltiplos fatores.
    
    Args:
        field_name: Nome da coluna
        df: DataFrame Spark
        json_type: Tipo do descritor JSON (se disponível)
        
    Returns:
        'numeric' ou 'categorical'
    """
    # Heurísticas baseadas no nome da coluna
    numeric_keywords = [
        'price', 'cost', 'value', 'amount', 'quantity', 'count',
        'total', 'sum', 'avg', 'mean', 'median', 'min', 'max',
        'rating', 'score', 'rate', 'percent', 'ratio', 'weight',
        'id', 'number', 'code', 'size', 'length', 'width',
        'height', 'depth', 'area', 'volume', 'temp', 'date',
        'year', 'month', 'day', 'hour', 'minute', 'second'
    ]
    
    categorical_keywords = [
        'name', 'type', 'category', 'status', 'flag', 'bool',
        'text', 'desc', 'description', 'comment', 'note',
        'url', 'link', 'path', 'key', 'code', 'enum',
        'list', 'array', 'json', 'xml', 'data'
    ]
    
    field_name_lower = field_name.lower()
    
    # Verificar keywords no nome
    has_numeric_keywords = any(keyword in field_name_lower for keyword in numeric_keywords)
    has_categorical_keywords = any(keyword in field_name_lower for keyword in categorical_keywords)
    
    if has_numeric_keywords and not has_categorical_keywords:
        return 'numeric'
    elif has_categorical_keywords and not has_numeric_keywords:
        return 'categorical'
    
    # Verificar se o tipo do JSON está disponível
    if json_type:
        json_type_lower = json_type.lower()
        if 'int' in json_type_lower or 'decimal' in json_type_lower or 'double' in json_type_lower or 'float' in json_type_lower:
            return 'numeric'
        elif 'string' in json_type_lower or 'text' in json_type_lower or 'varchar' in json_type_lower:
            return 'categorical'
    
    # Se não for possível determinar, usar validação de conteúdo
    try:
        is_content_numeric = _validate_numeric_content(df, field_name)
        return 'numeric' if is_content_numeric else 'categorical'
    except Exception:
        return 'categorical'  # Fallback seguro


def identify_column_types(df) -> Dict[str, List[str]]:
    """
    Identifica colunas numéricas e categóricas no DataFrame usando abordagem híbrida.
    
    Args:
        df: Spark DataFrame para analisar
        
    Returns:
        Dicionário com 'numeric' e 'categorical' keys contendo listas de colunas
    """
    numeric_columns = []
    categorical_columns = []
    
    for field in df.schema.fields:
        data_type = str(field.dataType).lower()
        
        # Usar detecção heurística combinada
        detected_type = _detect_column_type_heuristic(field.name, df)
        
        # Se a heurística detectar como numérico mas o schema já é numérico, confiar no schema
        if detected_type == 'numeric' and is_numeric_column(data_type):
            logger.info(f"Coluna '{field.name}' detectada como numérica (heurística + schema)")
            numeric_columns.append(field.name)
        elif detected_type == 'categorical':
            logger.info(f"Coluna '{field.name}' detectada como categórica (heurística)")
            categorical_columns.append(field.name)
        else:
            # Fallback para lógica original baseada no schema
            if is_numeric_column(data_type):
                logger.info(f"Coluna '{field.name}' classificada como numérica (schema)")
                numeric_columns.append(field.name)
            else:
                logger.info(f"Coluna '{field.name}' classificada como categórica (schema)")
                categorical_columns.append(field.name)
    
    logger.info(f"Detecção final: {len(numeric_columns)} numéricas, {len(categorical_columns)} categóricas")
    
    return {
        'numeric': numeric_columns,
        'categorical': categorical_columns
    }



def analyze_numeric_column(df, column_name: str) -> List[Dict[str, Any]]:
    """
    Realiza análise estatística em uma coluna numérica.
    
    Args:
        df: Spark DataFrame
        column_name: Nome da coluna numérica para analisar
        
    Returns:
        Lista de dicionários com resultados das métricas
    """
    try:
        # Calcular estatísticas usando approxPercentile para performance
        stats = df.select(
            F.mean(F.col(column_name)).alias("mean"),
            F.expr(f"percentile_approx({column_name}, 0.5)").alias("median"),
            F.min(F.col(column_name)).alias("min"),
            F.max(F.col(column_name)).alias("max"),
            F.expr(f"percentile_approx({column_name}, 0.25)").alias("q1"),
            F.expr(f"percentile_approx({column_name}, 0.75)").alias("q3"),
            F.expr(f"percentile_approx({column_name}, 0.95)").alias("p95"),
            F.expr(f"percentile_approx({column_name}, 0.99)").alias("p99"),
            F.count(F.col(column_name)).alias("non_null_count"),
            F.count(F.when(F.col(column_name).isNull(), 1)).alias("null_count"),
            F.count("*").alias("total_count")
        ).collect()[0]
        
        # Criar métricas no formato padrão
        metrics = [
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.MEAN.value,
                "metric_value": float(stats["mean"]) if stats["mean"] is not None else None,
                "metric_description": get_metric_description(MetricType.MEAN.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.MEDIAN.value,
                "metric_value": float(stats["median"]) if stats["median"] is not None else None,
                "metric_description": get_metric_description(MetricType.MEDIAN.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.MIN.value,
                "metric_value": float(stats["min"]) if stats["min"] is not None else None,
                "metric_description": get_metric_description(MetricType.MIN.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.MAX.value,
                "metric_value": float(stats["max"]) if stats["max"] is not None else None,
                "metric_description": get_metric_description(MetricType.MAX.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.Q1.value,
                "metric_value": float(stats["q1"]) if stats["q1"] is not None else None,
                "metric_description": get_metric_description(MetricType.Q1.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.Q3.value,
                "metric_value": float(stats["q3"]) if stats["q3"] is not None else None,
                "metric_description": get_metric_description(MetricType.Q3.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.P95.value,
                "metric_value": float(stats["p95"]) if stats["p95"] is not None else None,
                "metric_description": get_metric_description(MetricType.P95.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.P99.value,
                "metric_value": float(stats["p99"]) if stats["p99"] is not None else None,
                "metric_description": get_metric_description(MetricType.P99.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.NON_NULL_COUNT.value,
                "metric_value": float(stats["non_null_count"]) if stats["non_null_count"] is not None else None,
                "metric_description": get_metric_description(MetricType.NON_NULL_COUNT.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.NUMERIC.value,
                "metric_name": MetricType.NULL_COUNT.value,
                "metric_value": float(stats["null_count"]) if stats["null_count"] is not None else None,
                "metric_description": get_metric_description(MetricType.NULL_COUNT.value)
            }
        ]
        
        return metrics
        
    except Exception as e:
        logger.error(f"Erro ao analisar coluna numérica {column_name}: {str(e)}")
        return []


def analyze_categorical_column(df, column_name: str) -> List[Dict[str, Any]]:
    """
    Realiza análise estatística em uma coluna categórica.
    
    Args:
        df: Spark DataFrame
        column_name: Nome da coluna categórica para analisar
        
    Returns:
        Lista de dicionários com resultados das métricas
    """
    try:
        # Obter valores distintos e contagens
        distinct_df = df.select(column_name).distinct()
        distinct_count = distinct_df.count()
        
        # Obter valores distintos ordenados
        sorted_values = []
        value_counts = {}
        
        if distinct_count > 0:
            # Tentar ordenar numericamente, depois alfabeticamente
            try:
                sorted_rows = distinct_df.orderBy(F.col(column_name).cast("double")).collect()
                sorted_values = [row[column_name] for row in sorted_rows]
            except:
                sorted_rows = distinct_df.orderBy(column_name).collect()
                sorted_values = [row[column_name] for row in sorted_rows]
            
            # Obter contagens por valor
            value_counts_df = df.groupBy(column_name).count().orderBy(F.col("count").desc())
            value_counts = {row[column_name]: row["count"] for row in value_counts_df.collect()}
        
        # Calcular contagens de nulos/não nulos
        total_count = df.count()
        non_null_count = df.filter(F.col(column_name).isNotNull()).count()
        null_count = total_count - non_null_count
        
        metrics = [
            {
                "column_name": column_name,
                "column_type": ColumnType.CATEGORICAL.value,
                "metric_name": MetricType.DISTINCT_COUNT.value,
                "metric_value": float(distinct_count) if distinct_count is not None else None,
                "metric_description": get_metric_description(MetricType.DISTINCT_COUNT.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.CATEGORICAL.value,
                "metric_name": MetricType.DISTINCT_VALUES_SORTED.value,
                "metric_value": json.dumps(sorted_values, ensure_ascii=False),
                "metric_description": get_metric_description(MetricType.DISTINCT_VALUES_SORTED.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.CATEGORICAL.value,
                "metric_name": MetricType.VALUE_COUNTS.value,
                "metric_value": json.dumps(value_counts, ensure_ascii=False),
                "metric_description": get_metric_description(MetricType.VALUE_COUNTS.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.CATEGORICAL.value,
                "metric_name": MetricType.NON_NULL_COUNT.value,
                "metric_value": float(non_null_count) if non_null_count is not None else None,
                "metric_description": get_metric_description(MetricType.NON_NULL_COUNT.value)
            },
            {
                "column_name": column_name,
                "column_type": ColumnType.CATEGORICAL.value,
                "metric_name": MetricType.NULL_COUNT.value,
                "metric_value": float(null_count) if null_count is not None else None,
                "metric_description": get_metric_description(MetricType.NULL_COUNT.value)
            }
        ]
        
        return metrics
        
    except Exception as e:
        logger.error(f"Erro ao analisar coluna categórica {column_name}: {str(e)}")
        return []


def create_summary_dataframe(all_metrics: List[Dict[str, Any]]):
    """
    Cria DataFrame Spark a partir da lista de métricas.
    
    Args:
        all_metrics: Lista de dicionários com métricas
        
    Returns:
        Spark DataFrame com dados de sumário
    """
    # Verificar se spark está disponível (para testes fora do Databricks)
    if spark is None:
        # Mock para testes locais - retorna uma estrutura simulada
        class MockDataFrame:
            def __init__(self, data):
                self.data = data
            def count(self):
                return len(self.data)
            def show(self, n=20):
                for i, row in enumerate(self.data[:n]):
                    print(f"{i}: {row}")
        
        return MockDataFrame(all_metrics if all_metrics else [])
    
    if not all_metrics:
        # Criar DataFrame vazio com schema correto (compatível com Spark Connect)
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        
        empty_schema = StructType([
            StructField("column_name", StringType(), True),
            StructField("column_type", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_value", DoubleType(), True),  # DoubleType para valores numéricos
            StructField("metric_description", StringType(), True)
        ])
        
        return spark.createDataFrame([], empty_schema)
    
    # Manter valores como estão (já foram convertidos para float nas funções de análise)
    # Não é necessário conversão adicional pois os valores numéricos já são float
    
    # Criar DataFrame com schema explícito para evitar conflitos de tipo (compatível com Spark Connect)
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    
    rows = [Row(
        column_name=m["column_name"],
        column_type=m["column_type"],
        metric_name=m["metric_name"],
        metric_value=m["metric_value"],
        metric_description=m["metric_description"]
    ) for m in all_metrics]
    
    # Definir schema explicitamente para evitar conflitos
    schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("column_type", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True),  # DoubleType para acomodar todos os valores numéricos
        StructField("metric_description", StringType(), True)
    ])
    
    # Usar createDataFrame diretamente com a lista de Row e schema (compatível com Spark Connect)
    summary_df = spark.createDataFrame(rows, schema)
    
    return summary_df
