from datetime import datetime
from pathlib import Path
import csv

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.standard.operators.python import PythonOperator

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

DBT_PROJECT_PATH = (
    "/usr/local/airflow/include/dbt/mssql_project"
)

DBT_PROFILES_PATH = (
    "/usr/local/airflow/include/dbt/profiles.yml"
)

TEMP_TABLE = "dbo.temp_sales"


with DAG(
    dag_id="test_csv_dbt",
    start_date=datetime(2026, 9, 11),
    schedule=None,
    catchup=False,

) as dag:
      