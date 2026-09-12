from datetime import datetime
from pathlib import Path
import csv

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from cosmos import DbtTaskGroup
from cosmos import ProjectConfig
from cosmos import ProfileConfig
from cosmos import RenderConfig



# ============================================================
# CONFIGURATION
# ============================================================

CONNECTION_ID = "mssql_test"

CSV_FILE = Path(
    "/usr/local/airflow/include/data/incoming"
)

CSV_FILE_PATH =  "/usr/local/airflow/include/data/incoming" 

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

TEMP_TABLE = "dbo.temp_sales"


def check_files():

    if not CSV_FILE.exists():

            raise FileNotFoundError(
                f"Input folder does not exist: {CSV_FILE}"
            )

    csv_files = list(
            CSV_FILE.glob("*.csv")
        )

    print(csv_files)
    files=[]
    for file_path in csv_files:

            files.append(file_path.name)
            print(
                f"Checking file: {file_path.name}"
            )
    print("files: ",files)     
    return files


def start():
    print("start ...") 
    file_list=check_files()
    print("List recieved..")
    return file_list



def new_file_check(**context):
    file_list=context["ti"].xcom_pull(
        task_ids="start"
    )

    print("Try connection")
    hook = MsSqlHook(
                mssql_conn_id=CONNECTION_ID
            )
    print("connected....")

    new_files=[]     
    for filename in file_list:    
      sql=f"select count(*) from watermark where file_name ='{filename}';"
      print("SQL: ",sql)

      result=hook.get_first(sql)

      if result[0]==0:
       new_files.append(filename)

    print("File checking completed...")

    return new_files





with DAG(
    dag_id="dummy",
    start_date=datetime(2026, 9, 11),
    schedule=None,
    catchup=False,
) as dag:
   

    
   run_dbt = BashOperator(
        task_id="run_dbt_bronze_to_silver",

        bash_command=f"""
        set -e

        echo ""
        echo "dbt project directory:"
        cd {DBT_PROJECT_DIR}
        pwd

        echo ""
 
        echo "Running dbt Bronze -> Silver models..."
        dbt run \\
            --profiles-dir {DBT_PROFILES_DIR} \\
            --select dim_customer
        echo "dbt completed successfully"
        """,
    )
