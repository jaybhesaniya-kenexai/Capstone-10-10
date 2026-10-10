from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from datetime import datetime, timedelta

MSSQL_CONN_ID='mssql_test'
TOTAL_COLS = 52
# TOTAL_READ_LINES = ( 951 or 50000 )
CSV_FILE_PATH=" 'J:\\Airflow\\include\\data\\cricket_data.csv' "


DBT_PROJECT_PATH = (
    "/usr/local/airflow/include/dbt/mssql_project"
)

DBT_PROFILES_PATH = (
    "/usr/local/airflow/include/dbt/profiles.yml"
)

# dbt profiles.yml location
DBT_PROFILES_DIR = (
    "/usr/local/airflow/include/dbt"
)

# dbt project
DBT_PROJECT_DIR = (
    "/usr/local/airflow/include/dbt/mssql_project"
)

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def data_load_from_csv_job():

    hook = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )


    insertstmnt= f"""
             bulk insert bronze_cricket
             from  {CSV_FILE_PATH}
                with(
                format = 'CSV',
                lastrow=100000,
                firstrow=2
                ) ;
             """   

    print("INSERT STMNT: " , insertstmnt)

    print("\nINSERTING>>>>>>>>>>>>")
    hook.run(insertstmnt)
    print("Suceessedddd>>>>>>")


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

    data_load_from_csv=PythonOperator(
        task_id='data_load_from_csv',
        python_callable=data_load_from_csv_job
    )


    
    run_dbt = BashOperator(
        task_id="run_dbt_bronze_to_silver",

        bash_command=f"""
        set -e
        echo ""
        echo "dbt project directory:"
        cd {DBT_PROJECT_DIR}
        pwd 
        echo "Running dbt Bronze -> Silver models..."
        dbt run  --profiles-dir {DBT_PROFILES_DIR} 
        echo "dbt completed successfully"
        """,
    )

 
    end = EmptyOperator(
        task_id="end"
    )



    start >>  data_load_from_csv >> end