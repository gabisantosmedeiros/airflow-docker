import pendulum
from airflow import DAG
from airflow.sdk import task 

with DAG(
    dag_id="Exemplo_Xcom1",
    description="Xcom",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo","xcom"]
) as dag:
    @task
    def task_write():
        return {"valorxcom1" : 10000}
    
    @task
    def task_read(payload: dict):
        print(f"Valor retorno xcom:  {payload["valorxcom1"]}")

    task_read(task_write())