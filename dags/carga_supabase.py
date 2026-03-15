# -*- coding: utf-8 -*-
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- CREDENCIAIS ---
ID_PROJETO = "iwtublwcrevtzamfgzyq"
USUARIO_SUPABASE = f"postgres.{ID_PROJETO}"
SENHA_CORRETA = "Northwind@2025"
SENHA_URL = urllib.parse.quote_plus(SENHA_CORRETA)

# Mudamos o driver para 'pg8000' para ignorar problemas de biblioteca do sistema
URL_SUPABASE = f"postgresql+pg8000://{USUARIO_SUPABASE}:{SENHA_URL}@aws-0-sa-east-1.pooler.supabase.com:6543/postgres"

# TESTE LOCAL: Tente mudar a senha abaixo para 'postgres' se 'Northwind@2025' falhar
URL_LOCAL = f"postgresql+pg8000://postgres:{SENHA_URL}@host.docker.internal:5432/northwind"

def clonar_banco_completo():
    # Criamos os motores com exec_ut8 para blindar contra o erro do "ç"
    eng_local = create_engine(URL_LOCAL, client_encoding='utf8')
    eng_nuvem = create_engine(URL_SUPABASE, client_encoding='utf8')
    
    tabelas = [
        'categories', 'customers', 'employees', 'orders', 'products', 
        'shippers', 'suppliers', 'territories', 'region', 'order_details',
        'departamentos', 'funcionarios'
    ]

    print("--- INICIANDO MIGRACAO ---")
    
    for tab in tabelas:
        try:
            # Lendo com tratamento de erro de string
            query = f'SELECT * FROM public."{tab}"'
            df = pd.read_sql(query, eng_local)
            
            # Se o DF estiver vazio ou der erro, ele pula
            if not df.empty:
                df.to_sql(tab, eng_nuvem, if_exists='replace', index=False)
                print(f"TABELA {tab}: SUCESSO")
            else:
                print(f"TABELA {tab}: VAZIA")
                
        except Exception as e:
            # Limpamos a mensagem de erro para nao ter caracteres especiais que quebram o log
            err_msg = str(e).replace('ç', 'c').replace('ã', 'a')
            print(f"FALHA EM {tab}: Verifique se a senha local esta correta.")
            print(f"ERRO RESUMIDO: {err_msg[:100]}")

    eng_local.dispose()
    eng_nuvem.dispose()

with DAG(
    'MIGRACAO_TOTAL_DEFINITIVA_V3',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id='clonar_tudo',
        python_callable=clonar_banco_completo
    )