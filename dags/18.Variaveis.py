import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    dag_id="Variaveis",
    description="Ler Variáveis",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:

 
    def print_variable():
        minha_var = Variable.get("minhavar")
        print(f"O valor da variável é: {minha_var}")


    task1 = PythonOperator(task_id="tsk1", python_callable=print_variable)