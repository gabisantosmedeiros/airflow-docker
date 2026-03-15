from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging


logger = logging.getLogger("airflow.task")

def sync_data_task(table_name):
    """
    Função com log de erro.
    """
    try:
        logger.info(f"Iniciando atualização da tabela: {table_name}")
        
        
        print(f"Dados da tabela {table_name} atualizados com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro crítico ao atualizar {table_name}: {str(e)}")
        raise  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'UPDATE_SINC_DIARIA',
    default_args=default_args,
    description='Atualização automática diária de dados Northwind',
    schedule='@daily',  
    catchup=False,      
    tags=['producao', 'northwind'],
) as dag:

    
    with TaskGroup(group_id='northwind_sync_group') as sync_group:
        
        tables = [
            'categories', 
            'customers', 
            'employees', 
            'orders', 
            'products', 
            'shippers', 
            'suppliers'
        ]

        
        for table in tables:
            PythonOperator(
                task_id=f'sync_{table}',
                python_callable=sync_data_task,
                op_kwargs={'table_name': table},
            )

    sync_group  