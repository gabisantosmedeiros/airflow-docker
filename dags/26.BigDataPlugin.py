import pendulum
from airflow import DAG
from big_data_operator import BigDataOperator


with DAG(
    dag_id="bigdata",
    description="dinamico",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:
    big_data = BigDataOperator(
        task_id='big_data',
        path_to_csv_file="/opt/airflow/data/Churn.csv",
        path_to_save_file="/opt/airflow/data/Churn.parquet",
        file_type="parquet",
    )

    big_data