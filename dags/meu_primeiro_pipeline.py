from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    'processamento_northwind',
    start_date=datetime(2024, 1, 1),
    schedule=None, # Execução manual por enquanto
    catchup=False
) as dag:

    # Tarefa 1: Criar uma tabela de resumo no esquema contabilidade
    criar_tabela = SQLExecuteQueryOperator(
        task_id='criar_tabela_resumo',
        conn_id='postgres_local', # O ID que você criou
        sql="""
            CREATE TABLE IF NOT EXISTS contabilidade.resumo_categorias (
                categoria_nome VARCHAR(255),
                data_processamento TIMESTAMP
            );
        """
    )

    # Tarefa 2: Inserir dados vindos da tabela public.categories
    popular_resumo = SQLExecuteQueryOperator(
        task_id='popular_resumo',
        conn_id='postgres_local',
        sql="""
            INSERT INTO contabilidade.resumo_categorias (categoria_nome, data_processamento)
            SELECT category_name, CURRENT_TIMESTAMP 
            FROM public.categories;
        """
    )

    criar_tabela >> popular_resumo