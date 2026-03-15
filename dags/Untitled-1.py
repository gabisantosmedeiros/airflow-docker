# -*- coding: utf-8 -*-
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


HOST_NEON = "ep-red-brook-acdg14w2-pooler.sa-east-1.aws.neon.tech"
USUARIO_NEON = "neondb_owner"
SENHA_NEON = "npg_pMPLq5JlSZf2"
BANCO_NEON = "neondb"


URL_NEON = f"postgresql://{USUARIO_NEON}:{urllib.parse.quote_plus(SENHA_NEON)}@{HOST_NEON}/{BANCO_NEON}?sslmode=require"
URL_LOCAL = f"postgresql://postgres:{urllib.parse.quote_plus('123')}@host.docker.internal:5432/northwind"

def sync_data_final(source_schema, table, pk_col):
    eng_local = create_engine(URL_LOCAL)
    eng_neon = create_engine(URL_NEON)
    
    try:
      
        for df in pd.read_sql(f'SELECT * FROM {source_schema}."{table}"', eng_local, chunksize=1000):
            with eng_neon.begin() as conn:
                temp_table = f"temp_sync_{table}"
                df.to_sql(temp_table, conn, if_exists='replace', index=False)
                
                cols = [f'"{c}"' for c in df.columns]
                update_stmt = ", ".join([f'{c} = EXCLUDED.{c}' for c in cols if c != f'"{pk_col}"'])
                
               
                upsert_query = f"""
                    INSERT INTO public."{table}" ({", ".join(cols)})
                    SELECT * FROM "{temp_table}"
                    ON CONFLICT ("{pk_col}") 
                    DO UPDATE SET {update_stmt};
                """
                conn.execute(text(upsert_query))
                conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table}"'))
    finally:
        eng_local.dispose()
        eng_neon.dispose()


with DAG(
    'DAG_NORTHWIND_VERDE_TOTAL', 
    start_date=datetime(2026, 2, 1), 
    schedule='@daily', 
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)}
) as dag:

    with TaskGroup("sync_core") as sync_group:
        mapping = {
            'categories': 'category_id', 'customers': 'customer_id', 
            'employees': 'employee_id', 'products': 'product_id', 
            'shippers': 'shipper_id', 'suppliers': 'supplier_id', 'orders': 'order_id'
        }
        for t, pk in mapping.items():
            PythonOperator(
                task_id=f'sync_{t}', 
                python_callable=sync_data_final, 
                op_kwargs={'source_schema': 'public', 'table': t, 'pk_col': pk}
            )