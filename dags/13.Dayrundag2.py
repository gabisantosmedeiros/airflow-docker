import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dagrundag2",
    description="Dagrun2",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:

    task1 = BashOperator(task_id = "tsk1", bash_command='echo "{{ dag_run.conf["Chave"]}}"',)
    
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 5")

    task1 >> task2