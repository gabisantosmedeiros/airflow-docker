import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context
from airflow.providers.standard.operators.python import ShortCircuitOperator

with DAG(
    dag_id="shortcircuit",
    description="Teste de shortcircuit",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:

    def gerar_qualidade() -> int:
        return random.randint(0,100)
    
    gera_qualidade = PythonOperator(
        task_id='gera_qualidade',
        python_callable=gerar_qualidade,
    )

    def qualidade_suficiente() -> bool:
        ctx = get_current_context()
        ti = ctx['ti']
        qualidade = ti.xcom_pull(task_ids='gera_qualidade')
        return int(qualidade) >= 70
    
    shorcircuit = ShortCircuitOperator(
        task_id='shorcircuit',
        python_callable = qualidade_suficiente,
    )

    processa = BashOperator(
        task_id = 'processa',
        bash_command='echo Processado porque qualidade boa'
    )


    finaliza = BashOperator(
        task_id = 'finaliza',
        bash_command='echo Finalizado porque qualidade boa'
    )    
   

    gera_qualidade >> shorcircuit >> processa >> finaliza