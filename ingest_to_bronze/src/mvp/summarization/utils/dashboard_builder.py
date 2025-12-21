"""
Módulo principal para construção de dashboards interativos Databricks.

Este módulo contém a lógica principal para criar dashboards
com visualizações dinâmicas e interativas usando JSON/SQL.
"""

import json
import logging
from typing import Dict, Any, List, Optional

from databricks.sdk.runtime import spark

from .widget_creators import (
    create_bignumber_widgets,
    create_filter_widgets,
    create_numeric_visualizations,
    create_categorical_visualizations,
    create_drill_down_widgets
)
from .types import ColumnType

# Get logger for this module
logger = logging.getLogger(__name__)

# Mock spark para testes fora do Databricks
class MockResult:
    def __init__(self, data):
        self.data = data
    
    def collect(self):
        return self.data


class MockSpark:
    def sql(self, query):
        # Mock data for testing
        if "SELECT COUNT(*)" in query:
            from pyspark.sql import Row
            return MockResult([Row(row_count=1000)])
        elif "SELECT COUNT(DISTINCT column_name)" in query and "numeric" in query:
            from pyspark.sql import Row
            return MockResult([Row(numeric_columns=5)])
        elif "SELECT COUNT(DISTINCT column_name)" in query and "categorical" in query:
            from pyspark.sql import Row
            return MockResult([Row(categorical_columns=8)])
        elif "total_columns" in query:
            from pyspark.sql import Row
            return MockResult([Row(total_columns=13)])
        elif "SELECT DISTINCT column_name" in query and "numeric" in query:
            from pyspark.sql import Row
            return MockResult([Row(column_name=f"numeric_col_{i}") for i in range(5)])
        elif "SELECT DISTINCT column_name" in query and "categorical" in query:
            from pyspark.sql import Row
            return MockResult([Row(column_name=f"cat_col_{i}") for i in range(8)])
        elif "current_timestamp" in query:
            from pyspark.sql import Row
            import datetime
            # Criar um dicionário e depois converter para Row
            timestamp_data = {"current_timestamp()": datetime.datetime.now().isoformat()}
            return MockResult([Row(**timestamp_data)])
        else:
            from pyspark.sql import Row
            return MockResult([Row(metric_name="mean", metric_value=42.5), Row(metric_name="median", metric_value=40.0)])

# Try to get real spark, otherwise use mock
try:
    from databricks.sdk.runtime import spark
    if spark is None:
        spark = MockSpark()
except ImportError:
    spark = MockSpark()


class DashboardBuilder:
    """
    Classe responsável por construir dashboards interativos do Databricks.
    """
    
    def __init__(self):
        """
        Inicializa o construtor de dashboards.
        """
        self.logger = logging.getLogger(__name__)
    
    def create_interactive_dashboard(
        self, 
        catalog: str, 
        schema: str, 
        table: str,
        dashboard_name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        output_path: str = "/tmp"
    ) -> Dict[str, Any]:
        """
        Cria um dashboard interativo completo para análise de tabela.
        
        Args:
            catalog: Nome do catálogo
            schema: Nome do schema base
            table: Nome da tabela base
            dashboard_name: Nome personalizado para o dashboard
            tags: Lista de tags para o dashboard
            output_path: Caminho para salvar os arquivos gerados
            
        Returns:
            Dicionário com informações do dashboard criado
            
        Raises:
            Exception: Em caso de erro na criação do dashboard
        """
        try:
            # Validar existência da tabela de sumário
            self._validate_summary_table(catalog, schema, table)
            
            # Gerar nome do dashboard
            if not dashboard_name:
                dashboard_name = f"Análise Interativa - {table}"
            
            # Gerar tags padrão
            if not tags:
                tags = ["analise", "sumarizacao", table, "interativo"]
            
            # Criar widgets
            all_widgets = self._create_all_widgets(catalog, schema, table)
            
            # Criar configuração do dashboard
            dashboard_config = self._create_dashboard_config(
                dashboard_name, 
                f"Dashboard interativo para análise estatística da tabela {catalog}.{schema}.{table}",
                tags,
                all_widgets
            )
            
            # Criar script SQL
            sql_script = self._create_sql_script(catalog, schema, table, dashboard_name)
            
            # Salvar arquivos
            json_path = self._save_dashboard_json(dashboard_config, dashboard_name, output_path)
            sql_path = self._save_sql_script(sql_script, dashboard_name, output_path)
            
            # Gerar instruções de importação
            import_instructions = self._generate_import_instructions(json_path, sql_path)
            
            result = {
                "dashboard_name": dashboard_name,
                "dashboard_config": dashboard_config,
                "json_path": json_path,
                "sql_path": sql_path,
                "import_instructions": import_instructions,
                "widgets_count": len(all_widgets),
                "tags": tags
            }
            
            self.logger.info(f"Dashboard '{dashboard_name}' criado com sucesso. Arquivos salvos em {output_path}")
            return result
            
        except Exception as e:
            self.logger.error(f"Erro ao criar dashboard para {catalog}.{schema}.{table}: {str(e)}")
            raise
    
    def _validate_summary_table(self, catalog: str, schema: str, table: str) -> None:
        """
        Valida se a tabela de sumário existe e contém dados.
        
        Args:
            catalog: Nome do catálogo
            schema: Nome do schema base
            table: Nome da tabela base
            
        Raises:
            Exception: Se a tabela não existir ou estiver vazia
        """
        summary_table = f"{catalog}.{schema}_summary.{table}_summary"
        
        try:
            # Verificar se a tabela existe
            result = spark.sql(f"SELECT COUNT(*) as row_count FROM {summary_table} LIMIT 1").collect()
            
            if not result or len(result) == 0:
                raise Exception(f"Tabela de sumário {summary_table} não encontrada")
                
            row_count = result[0]["row_count"]
            if row_count == 0:
                raise Exception(f"Tabela de sumário {summary_table} está vazia")
                
            self.logger.info(f"Tabela de sumário {summary_table} validada com {row_count} registros")
            
        except Exception as e:
            if "not found" in str(e).lower() or "doesn't exist" in str(e).lower():
                raise Exception(f"Tabela de sumário {summary_table} não existe. Execute primeiro a task de sumarização.")
            raise
    
    def _create_all_widgets(self, catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
        """
        Cria todos os widgets para o dashboard.
        
        Args:
            catalog: Nome do catálogo
            schema: Nome do schema base
            table: Nome da tabela base
            
        Returns:
            Lista completa de widgets
        """
        all_widgets = []
        
        try:
            # Big numbers do topo
            self.logger.info("Criando big numbers...")
            bignumber_widgets = create_bignumber_widgets(catalog, schema, table)
            all_widgets.extend(bignumber_widgets)
            
            # Filtros interativos
            self.logger.info("Criando filtros interativos...")
            filter_widgets = create_filter_widgets()
            all_widgets.extend(filter_widgets)
            
            # Visualizações para colunas numéricas
            self.logger.info("Criando visualizações para colunas numéricas...")
            numeric_widgets = create_numeric_visualizations(catalog, schema, table)
            all_widgets.extend(numeric_widgets)
            
            # Visualizações para colunas categóricas
            self.logger.info("Criando visualizações para colunas categóricas...")
            categorical_widgets = create_categorical_visualizations(catalog, schema, table)
            all_widgets.extend(categorical_widgets)
            
            # Widgets de drill-down
            self.logger.info("Criando widgets de drill-down...")
            drill_down_widgets = create_drill_down_widgets(catalog, schema, table)
            all_widgets.extend(drill_down_widgets)
            
            self.logger.info(f"Total de {len(all_widgets)} widgets criados")
            return all_widgets
            
        except Exception as e:
            self.logger.error(f"Erro ao criar widgets: {str(e)}")
            raise
    
    def _create_dashboard_config(
        self, 
        name: str, 
        description: str, 
        tags: List[str], 
        widgets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Cria a configuração do dashboard no formato JSON.
        
        Args:
            name: Nome do dashboard
            description: Descrição do dashboard
            tags: Lista de tags
            widgets: Lista de widgets
            
        Returns:
            Dicionário com configuração do dashboard
        """
        # Criar configuração do dashboard
        dashboard_config = {
            "name": name,
            "description": description,
            "tags": tags,
            "widgets": widgets,
            "layout": {
                "style": "GRID",
                "auto_refresh": True,
                "refresh_interval": 300  # 5 minutos
            },
            "created_at": spark.sql("SELECT current_timestamp()").collect()[0]["current_timestamp()"],
            "version": "1.0"
        }
        
        return dashboard_config
    
    def _create_sql_script(
        self, 
        catalog: str, 
        schema: str, 
        table: str,
        dashboard_name: str
    ) -> str:
        """
        Cria um script SQL com todas as queries do dashboard.
        
        Args:
            catalog: Nome do catálogo
            schema: Nome do schema base
            table: Nome da tabela base
            dashboard_name: Nome do dashboard
            
        Returns:
            Script SQL completo
        """
        summary_table = f"{catalog}.{schema}_summary.{table}_summary"
        
        script = f"""-- Script SQL para Dashboard: {dashboard_name}
-- Tabela base: {catalog}.{schema}.{table}
-- Tabela de sumário: {summary_table}
-- Gerado em: {spark.sql('SELECT current_timestamp()').collect()[0]['current_timestamp()']}

-- ============================================================================
-- CONFIGURAÇÃO DO DASHBOARD
-- ============================================================================

-- 1. Criar Parâmetros do Dashboard
CREATE OR REPLACE TEMPORARY VIEW dashboard_params AS
SELECT 
    '{catalog}' as catalog_name,
    '{schema}' as schema_name,
    '{table}' as table_name,
    '{summary_table}' as summary_table;

-- 2. Big Numbers do Topo
-- ============================================================================
"""

        # Adicionar queries dos big numbers
        widgets = self._create_all_widgets(catalog, schema, table)
        
        for i, widget in enumerate(widgets):
            if widget["visualization"]["type"] == "big_number":
                script += f"""
-- {widget['title']}
-- {widget['description']}
{widget['visualization']['query']}

"""

        script += """
-- 3. Visualizações por Tipo de Coluna
-- ============================================================================
"""

        # Adicionar queries das outras visualizações
        for widget in widgets:
            if widget["visualization"]["type"] != "big_number":
                script += f"""
-- {widget['title']}
-- {widget['description']}
-- Tipo: {widget['visualization']['type']}
{widget['visualization']['query']}

"""

        script += """
-- 4. Consultas de Validação e Debug
-- ============================================================================

-- Verificar tabela de sumário
SELECT * FROM dashboard_params;

-- Contagem de registros por tipo de coluna
SELECT 
    column_type,
    COUNT(DISTINCT column_name) as column_count,
    COUNT(*) as metric_count
FROM {{summary_table}}
GROUP BY column_type
ORDER BY column_type;

-- Verificar cobertura de métricas
SELECT 
    metric_name,
    COUNT(DISTINCT column_name) as columns_with_metric
FROM {{summary_table}}
GROUP BY metric_name
ORDER BY metric_name;

"""

        return script
    
    def _save_dashboard_json(
        self, 
        dashboard_config: Dict[str, Any], 
        dashboard_name: str, 
        output_path: str
    ) -> str:
        """
        Salva a configuração do dashboard em arquivo JSON.
        
        Args:
            dashboard_config: Configuração do dashboard
            dashboard_name: Nome do dashboard
            output_path: Caminho de saída
            
        Returns:
            Caminho do arquivo salvo
        """
        import os
        
        # Limpar nome do arquivo
        safe_name = dashboard_name.replace(" ", "_").replace("-", "_").lower()
        filename = f"dashboard_{safe_name}.json"
        filepath = os.path.join(output_path, filename)
        
        # Salvar usando dbutils
        json_content = json.dumps(dashboard_config, indent=2, ensure_ascii=False)
        
        try:
            dbutils.fs.put(f"file:{filepath}", json_content, overwrite=True)
            self.logger.info(f"Configuração JSON salva em: {filepath}")
        except:
            # Fallback para escrita local
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(json_content)
            self.logger.info(f"Configuração JSON salva localmente em: {filepath}")
        
        return filepath
    
    def _save_sql_script(
        self, 
        sql_script: str, 
        dashboard_name: str, 
        output_path: str
    ) -> str:
        """
        Salva o script SQL do dashboard.
        
        Args:
            sql_script: Script SQL
            dashboard_name: Nome do dashboard
            output_path: Caminho de saída
            
        Returns:
            Caminho do arquivo salvo
        """
        import os
        
        # Limpar nome do arquivo
        safe_name = dashboard_name.replace(" ", "_").replace("-", "_").lower()
        filename = f"dashboard_{safe_name}.sql"
        filepath = os.path.join(output_path, filename)
        
        try:
            # Salvar usando dbutils
            dbutils.fs.put(f"file:{filepath}", sql_script, overwrite=True)
            self.logger.info(f"Script SQL salvo em: {filepath}")
        except:
            # Fallback para escrita local
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(sql_script)
            self.logger.info(f"Script SQL salvo localmente em: {filepath}")
        
        return filepath
    
    def _generate_import_instructions(self, json_path: str, sql_path: str) -> List[str]:
        """
        Gera instruções para importar o dashboard no Databricks.
        
        Args:
            json_path: Caminho do arquivo JSON
            sql_path: Caminho do arquivo SQL
            
        Returns:
            Lista de instruções
        """
        instructions = [
            f"1. Para criar o dashboard interativo:",
            f"   - Abra o SQL Editor do Databricks",
            f"   - Execute as queries do arquivo: {sql_path}",
            f"   - Use os resultados para criar visualizações individuais",
            "",
            f"2. Para importar a configuração completa:",
            f"   - Vá para Dashboards no Databricks",
            f"   - Clique em 'Import Dashboard'",
            f"   - Selecione o arquivo: {json_path}",
            "",
            f"3. Configurações manuais recomendadas:",
            f"   - Atualização automática: 5 minutos",
            f"   - Layout: Grid",
            f"   - Permissões: Compartilhar com a equipe",
            "",
            f"4. Para drill-down interativo:",
            f"   - Configure parâmetros de dashboard",
            f"   - Crie filtros vinculados entre visualizações",
            f"   - Habilite cliques para detalhamento"
        ]
        
        return instructions


def create_interactive_dashboard(
    catalog: str, 
    schema: str, 
    table: str,
    dashboard_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    output_path: str = "/tmp"
) -> Dict[str, Any]:
    """
    Função conveniente para criar um dashboard interativo.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema base
        table: Nome da tabela base
        dashboard_name: Nome personalizado para o dashboard
        tags: Lista de tags para o dashboard
        output_path: Caminho para salvar os arquivos gerados
        
    Returns:
        Dicionário com informações do dashboard criado
    """
    builder = DashboardBuilder()
    return builder.create_interactive_dashboard(
        catalog=catalog,
        schema=schema,
        table=table,
        dashboard_name=dashboard_name,
        tags=tags,
        output_path=output_path
    )
