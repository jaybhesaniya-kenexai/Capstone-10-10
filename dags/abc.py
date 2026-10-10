from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="mssql_dbt_dummy",
    default_args=default_args,
    start_date=datetime(2026, 10, 9),
    schedule=None,
    catchup=False,
    tags=["dbt", "mssql"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )
 
    end = EmptyOperator(
        task_id="end"
    )

    start >> end