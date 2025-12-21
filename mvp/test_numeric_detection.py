#!/usr/bin/env python3
"""
Teste para validação da detecção de tipos numéricos vs categóricos
"""

import logging
from typing import Dict, Any, List

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_numeric_detection():
    """
    Testa a detecção de tipos numéricos usando dados simulados
    """
    print("🧪 Testando detecção de tipos numéricos vs categóricos...")
    
    # Dados de teste com diferentes tipos de campos
    test_data = [
        {"price": "19.99", "name": "Product A", "rating": "4.5", "description": "Good product"},
        {"price": "29.99", "name": "Product B", "rating": "3.2", "description": "Average product"},
        {"price": "invalid_price", "name": "Product C", "rating": "5.0", "description": "Excellent product"},
        {"price": "39.99", "name": "Product D", "rating": "not_a_number", "description": "Premium product"},
    ]
    
    try:
        # Simular Spark DataFrame
        import sys
        sys.path.append('src')
        
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType
        
        # Criar sessão Spark local para teste
        spark = SparkSession.builder.appName("test_numeric_detection").getOrCreate()
        
        # Criar schema
        schema = StructType([
            StructField("price", StringType(), True),
            StructField("name", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("description", StringType(), True),
        ])
        
        # Criar DataFrame
        from pyspark.sql import Row
        rows = [Row(**row) for row in test_data]
        df = spark.createDataFrame(rows, schema)
        
        print("✅ DataFrame de teste criado:")
        df.show()
        
        # Importar funções de análise
        from mvp.summarization.utils.analyzers import identify_column_types, _validate_numeric_content
        
        # Testar identificação de tipos
        print("\n🔍 Testando identificação de tipos:")
        column_types = identify_column_types(df)
        
        print(f"Colunas numéricas detectadas: {column_types['numeric']}")
        print(f"Colunas categóricas detectadas: {column_types['categorical']}")
        
        # Testar validação de conteúdo para cada coluna
        print("\n🔬 Testando validação de conteúdo por coluna:")
        for field in df.schema.fields:
            column_name = field.name
            is_numeric = _validate_numeric_content(df, column_name)
            print(f"  {column_name}: {'✅ Numérico' if is_numeric else '❌ Não numérico'}")
        
        # Análise esperada
        print("\n📊 Análise esperada:")
        print("  - price: Deveria ser categórico (contém 'invalid_price')")
        print("  - name: Deveria ser categórico (texto)")  
        print("  - rating: Deveria ser categórico (contém 'not_a_number')")
        print("  - description: Deveria ser categórico (texto)")
        
        # Verificar se a detecção funcionou corretamente
        expected_categorical = {'name', 'description', 'price', 'rating'}  # Todos devem ser categóricos devido a dados mistos
        actual_categorical = set(column_types['categorical'])
        
        print(f"\n✅ Validação final:")
        print(f"  Esperado categórico: {sorted(expected_categorical)}")
        print(f"  Detectado categórico: {sorted(actual_categorical)}")
        
        if expected_categorical == actual_categorical:
            print("  🎉 Detecção funcionou corretamente!")
        else:
            print("  ⚠️  Detecção precisa de ajustes")
        
        spark.stop()
        return expected_categorical == actual_categorical
        
    except Exception as e:
        print(f"❌ Erro no teste: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_numeric_detection()
    print(f"\n🏁 Teste {'concluído com sucesso' if success else 'falhou'}")
