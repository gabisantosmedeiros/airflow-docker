import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="daggroup",
    description="Task Group",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo","complexa"]
) as dag:
    task1 = BashOperator(task_id='tsk1', bash_command="sleep 5")
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 1")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 8")        
    task4 = BashOperator(task_id='tsk4', bash_command="sleep 5")        
    task5 = BashOperator(task_id='tsk5', bash_command="sleep 5")        
    task6 = BashOperator(task_id='tsk6', bash_command="sleep 5")        
    
    with TaskGroup(group_id="tsk_group") as tsk_group:
        task7 = BashOperator(task_id='tsk7', bash_command="sleep 5")        
        task8 = BashOperator(task_id='tsk8', bash_command="sleep 5")        
        task9 = BashOperator(task_id='tsk9', bash_command="sleep 5")                            

    task1 >> task2
    task3 >> task4
    [task2,task4] >> task5 >> task6
    task6 >> tsk_group