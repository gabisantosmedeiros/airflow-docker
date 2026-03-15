import pendulum 
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past": False,
    "email": ["teste@email.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="Defaultargs",
    description="Dag teste default args",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:
    task1 = BashOperator(task_id='tsk1', bash_command="exit 1", retries=3)
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 5")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 5")

    task1 >> task2 >> task3