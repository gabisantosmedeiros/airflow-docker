import pendulum
from airflow import DAG
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="teste_email",
    description="Envio de Email",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:
    EmailOperator(
        task_id="send_email",
        to=['contato@evoluth.com.br'],
        subject="Teste de Email no Airflow",
        html_content="<p>Email Enviado</p>",

    ) 