import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="triggerdag1",
    description="Trigger Dag 1",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:
    task1 = BashOperator(task_id='tsk1', bash_command="sleep 5")
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 5")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 5", trigger_rule=TriggerRule.ONE_FAILED)        

    [task1 , task2 ] >> task3 