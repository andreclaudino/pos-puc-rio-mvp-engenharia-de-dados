#!/usr/bin/env python3
"""
Test script for dashboard creation functionality.
"""

import sys
import os
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_dashboard_creation():
    """Test the dashboard creation functionality."""
    print("Testando criação de dashboard interativo...")
    
    # Create test directory
    test_dir = '/tmp/dashboard_test_final'
    os.makedirs(test_dir, exist_ok=True)
    
    try:
        # Import dashboard builder
        from src.mvp.summarization.utils.dashboard_builder import create_interactive_dashboard
        print("✅ Importação do dashboard_builder funcionando!")
        
        # Test dashboard creation
        result = create_interactive_dashboard(
            catalog='test_catalog',
            schema='test_schema',
            table='test_table',
            dashboard_name='Dashboard Teste Final',
            tags=['teste', 'dashboard'],
            output_path=test_dir
        )
        
        print(f"\n=== RESULTADO DO DASHBOARD ===")
        print(f"Dashboard Name: {result.get('dashboard_name', 'N/A')}")
        print(f"Widgets Count: {result.get('widgets_count', 0)}")
        print(f"JSON Path: {result.get('json_path')}")
        print(f"SQL Path: {result.get('sql_path')}")
        
        # Check if files were created
        json_path = result.get('json_path')
        sql_path = result.get('sql_path')
        
        if json_path and os.path.exists(json_path):
            print(f"✅ Arquivo JSON criado: {json_path}")
            # Show partial content
            with open(json_path, 'r', encoding='utf-8') as f:
                content = f.read()
                print(f"Conteúdo JSON (primeiros 200 chars): {content[:200]}...")
        else:
            print(f"❌ Arquivo JSON não encontrado: {json_path}")
        
        if sql_path and os.path.exists(sql_path):
            print(f"✅ Arquivo SQL criado: {sql_path}")
            # Show partial content
            with open(sql_path, 'r', encoding='utf-8') as f:
                content = f.read()
                print(f"Conteúdo SQL (primeiros 200 chars): {content[:200]}...")
        else:
            print(f"❌ Arquivo SQL não encontrado: {sql_path}")
        
        # Show import instructions
        if 'import_instructions' in result:
            print(f"\n=== INSTRUÇÕES DE IMPORTAÇÃO ===")
            for instruction in result['import_instructions']:
                print(f"📋 {instruction}")
        
        print("\n✅ Dashboard interativo testado com sucesso!")
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_dashboard_creation()
    sys.exit(0 if success else 1)
