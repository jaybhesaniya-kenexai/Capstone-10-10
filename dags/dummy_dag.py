from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from datetime import datetime, timedelta

MSSQL_CONN_ID='mssql_test'
TOTAL_COLS = 52
TOTAL_READ_LINES = ( 951 or 50000 )



default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def total_csv_Records(file_name):
    with open(file_name) as f:
        cnt=0
        while f.readline() :
            cnt+=1
        return cnt   



    # print("Connecting hook...")
    # hook.run( "select '1' ")
    # print('hook connected...')
     



with DAG(
    dag_id="BULK_LOAD_DAG",
    default_args=default_args,
    start_date=datetime(2026, 10, 9),
    schedule=None,
    catchup=False,
    tags=["dbt", "mssql"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    data_load_from_csv=PythonOperator(
        task_id='data_load_from_csv',
        python_callable=data_load_from_csv_job
    )
 
    end = EmptyOperator(
        task_id="end"
    )

    start >>  data_load_from_csv >> end