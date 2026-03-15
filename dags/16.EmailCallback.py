import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email

def on_fail(ctx):
    send_email(
        to=['contato@evoluth.com.br'],
        subject=f"[FAIL] {ctx['ti'].task_id}",
        html_content=f'Falhou',
    )

def on_ok(ctx):    
    send_email(
        to=['contato@evoluth.com.br'],
        subject=f"[OK] {ctx['ti'].task_id}",
        html_content=f'Sucesso!',
    )
    


default_args = {
    "depends_on_past" : False,
    "email": ["contato@evoluth.com.br"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="Email_callback",
    description="Teste de error e retry",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Curso","Email"],
) as dag:
    task1 = BashOperator(task_id='tsk1', bash_command="exit 1", on_failure_callback=on_fail)
    
    
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 5", on_success_callback=on_ok,
                         trigger_rule=TriggerRule.ALL_DONE  )
      

    task1 >> task2  