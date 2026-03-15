import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past" : False,
    "email": ["contato@evoluth.com.br"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="Email_error_retry",
    description="Teste de error e retry",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Curso","Email"],
) as dag:
    task1 = BashOperator(task_id='tsk1', bash_command="exit 1")
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 5")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 5")        

    task1 >> task2  >> task3 