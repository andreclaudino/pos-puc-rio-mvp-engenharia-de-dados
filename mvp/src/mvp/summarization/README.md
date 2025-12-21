# Table Summarization Module

Este módulo oferece funcionalidades para análise estatística automática de tabelas no Databricks, gerando sumários detalhados e dashboards para visualização.

## Componentes

### 1. Table Summarization (`table_summarization.py`)

Script principal que realiza análise estatística completa de tabelas do Databricks.

#### Funcionalidades

**Análise de Colunas Numéricas:**
- Média
- Mediana
- Valor mínimo e máximo
- Primeiro quartil (Q1)
- Terceiro quartil (Q3)
- Percentil 95 (P95)
- Percentil 99 (P99)
- Contagem de valores nulos e não nulos

**Análise de Colunas Categóricas:**
- Total de valores distintos
- Lista de valores distintos ordenados
- Contagem de ocorrências por categoria (frequency table)
- Contagem de valores nulos e não nulos

#### Uso

```bash
# Via variáveis de ambiente
export CATALOG="seu_catalog"
export SCHEMA="seu_schema"
export TABLE="sua_tabela"
table_summarization

# Ou via parâmetros de linha de comando
table_summarization --catalog "seu_catalog" --schema "seu_schema" --table "sua_tabela"

# Com modo de escrita específico
table_summarization --catalog "seu_catalog" --schema "seu_schema" --table "sua_tabela" --write-mode "append"
```

#### Parâmetros

- `--catalog` (obrigatório): Nome do catálogo do Databricks
- `--schema` (obrigatório): Nome do schema
- `--table` (obrigatório): Nome da tabela a ser analisada
- `--write-mode` (opcional): Modo de escrita (`overwrite` ou `append`, padrão: `overwrite`)

#### Saída

O script cria uma tabela de sumário com o nome `{nome_tabela}_summary` no schema `{nome_schema}_summary` com a seguinte estrutura:

| Coluna | Descrição |
|--------|-----------|
| `column_name` | Nome da coluna analisada |
| `column_type` | Tipo da coluna (numeric/categorical) |
| `metric_name` | Nome da métrica estatística |
| `metric_value` | Valor da métrica |
| `metric_description` | Descrição do que representa a métrica |

### 2. Dashboard Creation (`create_dashboard.py`)

Script que gera configurações de dashboard para visualização dos dados de sumarização.

#### Funcionalidades

- Gera queries SQL para visualizações automáticas
- Cria configuração JSON para dashboards
- Produz script SQL completo para uso manual

#### Visualizações Incluídas

1. **Visão Geral das Colunas** - Tabela resumo com tipos e contagens
2. **Distribuição de Valores Nulos** - Gráfico de barras com percentual de nulos
3. **Estatísticas Descritivas** - Tabela com média, mediana, quartis para colunas numéricas
4. **Análise de Percentis** - Comparação P95 vs P99
5. **Cardinalidade** - Número de valores distintos por coluna categórica
6. **Valores Mais Frequentes** - Top valores por categoria
7. **Resumo de Métricas** - Distribuição de tipos de métricas

#### Uso

```bash
# Uso básico
create_dashboard --catalog "seu_catalog" --schema "seu_schema" --table "sua_tabela"

# Gerar apenas arquivo SQL
create_dashboard --catalog "seu_catalog" --schema "seu_schema" --table "sua_tabela" --output-format "sql"

# Especificar diretório de saída
create_dashboard --catalog "seu_catalog" --schema "seu_schema" --table "sua_tabela" --output-path "/tmp/dashboards"
```

#### Parâmetros

- `--catalog` (obrigatório): Nome do catálogo
- `--schema` (obrigatório): Nome do schema
- `--table` (obrigatório): Nome da tabela base (sem o sufixo `_summary`)
- `--output-format` (opcional): Formato de saída (`json`, `sql`, `both`, padrão: `both`)
- `--output-path` (opcional): Diretório de saída (padrão: `/tmp`)

## Fluxo de Trabalho Recomendado

### 1. Análise da Tabela

```bash
# Gerar sumário estatístico
table_summarization --catalog "bronze" --schema "products" --table "default_product_info"

# Resultado: tabela bronze.products_summary.default_product_info_summary
```

### 2. Criação do Dashboard

```bash
# Gerar configurações do dashboard
create_dashboard --catalog "bronze" --schema "products" --table "default_product_info"

# Resultado: arquivos SQL e JSON com queries para visualização
```

### 3. Visualização no Databricks

1. Abra o SQL Editor no Databricks
2. Execute as queries geradas no arquivo SQL
3. Crie visualizações usando os resultados
4. Monte um dashboard com os gráficos gerados

## Exemplo de Consultas Diretas

Depois de gerar a tabela de sumário, você pode consultar diretamente:

```sql
-- Ver todas as métricas de uma coluna específica
SELECT metric_name, metric_value, metric_description
FROM bronze.products_summary.default_product_info_summary
WHERE column_name = 'search_price' AND column_type = 'numeric'
ORDER BY metric_name;

-- Encontrar colunas com muitos valores nulos
SELECT 
    column_name,
    column_type,
    MAX(CASE WHEN metric_name = 'null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as null_count,
    MAX(CASE WHEN metric_name = 'non_null_count' THEN CAST(metric_value AS INTEGER) ELSE 0 END) as non_null_count
FROM bronze.products_summary.default_product_info_summary
WHERE metric_name IN ('null_count', 'non_null_count')
GROUP BY column_name, column_type
HAVING null_count > 0
ORDER BY null_count DESC;

-- Análise de cardinalidade para colunas categóricas
SELECT 
    column_name,
    CAST(metric_value AS INTEGER) as distinct_values
FROM bronze.products_summary.default_product_info_summary
WHERE column_type = 'categorical' AND metric_name = 'distinct_count'
ORDER BY distinct_values DESC;
```

## Considerações de Performance

- O script usa `approxPercentile` para cálculos de percentis em grandes conjuntos de dados
- Operações são otimizadas para minimizar o número de jobs Spark
- Para tabelas muito grandes (>100M registros), considere executar durante períodos de baixa demanda

## Tratamento de Erros

- Colunas que não podem ser convertidas para análise são ignoradas com logging apropriado
- Erros em colunas individuais não interrompem o processamento das demais
- Logs detalhados são gerados para debugging

## Extensões Futuras

- Suporte a tipos de dados adicionais (timestamps, arrays, structs)
- Opções de sampling para análise em tabelas extremamente grandes
- Integração automática com dashboards do Databricks via API
- Análise de correlação entre colunas numéricas
- Detecção automática de outliers
