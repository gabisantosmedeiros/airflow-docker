import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context

with DAG(
    dag_id="Exemplo_Xcom2",
    description="Xcom2",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo","xcom"]
) as dag:

    def task_write():
        ti = get_current_context()['ti']
        ti.xcom_push(key='Valorxcom1', value=10000)

    def task_read():
        ti = get_current_context()['ti']
        valor = ti.xcom_pull(key='Valorxcom1', task_ids='tsk1')                
        print(f"Valor recuperado : {valor}")

    task1 = PythonOperator(task_id='tsk1', python_callable=task_write)
    task2 = PythonOperator(task_id='tsk2', python_callable=task_read)    

    task1 >> task2
    