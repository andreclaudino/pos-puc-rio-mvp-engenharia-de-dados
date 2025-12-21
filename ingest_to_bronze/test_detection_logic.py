#!/usr/bin/env python3
"""
Teste para validação da lógica de detecção sem dependência do Spark
"""

import logging
import sys
import os

# Adicionar o diretório src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_heuristic_detection():
    """
    Testa a detecção heurística de tipos de coluna
    """
    print("🧪 Testando detecção heurística de tipos...")
    
    # Importar funções de análise
    try:
        from mvp.summarization.utils.analyzers import _detect_column_type_heuristic
        from mvp.summarization.utils.types import is_numeric_column
    except ImportError as e:
        print(f"❌ Erro ao importar funções: {e}")
        return False
    
    # Casos de teste para heurística
    test_cases = [
        # (nome_coluna, json_type, resultado_esperado, descricao)
        ("price", "string", "numeric", "Campo com keyword numérica"),
        ("search_price", "decimal", "numeric", "Campo com keyword numérica"),
        ("product_name", "string", "categorical", "Campo com keyword categórica"),
        ("description", "text", "categorical", "Campo com keyword categórica"),
        ("rating", "float", "numeric", "Campo numérico no JSON"),
        ("id", "bigint", "numeric", "Campo ID numérico"),
        ("url", "varchar", "categorical", "Campo URL categórico"),
        ("merchant_category", "string", "categorical", "Campo category categórico"),
        ("amount", "decimal", "numeric", "Campo amount numérico"),
        ("status", "string", "categorical", "Campo status categórico"),
        ("unknown_field", None, "categorical", "Campo desconhecido (fallback)"),
    ]
    
    print("\n📋 Testes de detecção heurística:")
    success_count = 0
    total_tests = len(test_cases)
    
    for field_name, json_type, expected, description in test_cases:
        try:
            # Testar detecção heurística (sem DataFrame para este teste)
            result = _detect_column_type_heuristic(field_name, None, json_type)
            
            status = "✅" if result == expected else "❌"
            print(f"  {status} {field_name} ({json_type}): {result} (esperado: {expected}) - {description}")
            
            if result == expected:
                success_count += 1
                
        except Exception as e:
            print(f"  ❌ {field_name}: Erro - {str(e)}")
    
    accuracy = (success_count / total_tests) * 100
    print(f"\n📊 Precisão da detecção heurística: {accuracy:.1f}% ({success_count}/{total_tests})")
    
    return accuracy >= 80  # Considerar sucesso se >= 80% de precisão


def test_numeric_column_detection():
    """
    Testa a função is_numeric_column com diferentes tipos de dados
    """
    print("\n🔬 Testando detecção de tipos numéricos de schema...")
    
    try:
        from mvp.summarization.utils.types import is_numeric_column
    except ImportError as e:
        print(f"❌ Erro ao importar is_numeric_column: {e}")
        return False
    
    # Tipos de dados para teste
    test_types = [
        ("integer", True, "Tipo inteiro deve ser numérico"),
        ("long", True, "Tipo long deve ser numérico"),
        ("double", True, "Tipo double deve ser numérico"),
        ("float", True, "Tipo float deve ser numérico"),
        ("decimal", True, "Tipo decimal deve ser numérico"),
        ("string", False, "Tipo string não deve ser numérico"),
        ("varchar", False, "Tipo varchar não deve ser numérico"),
        ("text", False, "Tipo text não deve ser numérico"),
        ("boolean", False, "Tipo boolean não deve ser numérico"),
        ("date", False, "Tipo date não deve ser numérico"),
        ("timestamp", False, "Tipo timestamp não deve ser numérico"),
    ]
    
    print("\n📋 Testes de detecção de schema:")
    success_count = 0
    total_tests = len(test_types)
    
    for data_type, expected, description in test_types:
        try:
            result = is_numeric_column(data_type)
            status = "✅" if result == expected else "❌"
            print(f"  {status} {data_type}: {result} (esperado: {expected}) - {description}")
            
            if result == expected:
                success_count += 1
                
        except Exception as e:
            print(f"  ❌ {data_type}: Erro - {str(e)}")
    
    accuracy = (success_count / total_tests) * 100
    print(f"\n📊 Precisão da detecção de schema: {accuracy:.1f}% ({success_count}/{total_tests})")
    
    return accuracy >= 90  # Considerar sucesso se >= 90% de precisão


def main():
    """
    Função principal de testes
    """
    print("🚀 Iniciando testes de detecção de tipos numéricos vs categóricos")
    print("=" * 60)
    
    # Testar detecção heurística
    heuristic_success = test_heuristic_detection()
    
    # Testar detecção de schema
    schema_success = test_numeric_column_detection()
    
    # Resultado geral
    print("\n" + "=" * 60)
    print("📈 RESUMO DOS TESTES:")
    print(f"  Detecção Heurística: {'✅ Aprovado' if heuristic_success else '❌ Reprovado'}")
    print(f"  Detecção de Schema: {'✅ Aprovado' if schema_success else '❌ Reprovado'}")
    
    overall_success = heuristic_success and schema_success
    print(f"\n🏁 Resultado Final: {'✅ TESTES APROVADOS' if overall_success else '❌ TESTES REPROVADOS'}")
    
    return overall_success


if __name__ == "__main__":
    main()
