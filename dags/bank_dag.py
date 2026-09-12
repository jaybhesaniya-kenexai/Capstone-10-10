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

CSV_FILE_PATH =  "/usr/local/airflow/include/data/incoming" 

DBT_PROJECT_PATH = (
    "/usr/local/airflow/include/dbt/mssql_project"
)

DBT_PROFILES_PATH = (
    "/usr/local/airflow/include/dbt/profiles.yml"
)


table_name = 'banking_data_raw'


MAX_ROWS_TO_LOAD = 500000

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
       print("file added: ",filename)
       new_files.append(filename)

    print("File checking completed...")

    return new_files


def load_csv_files(**context):
    file_list=context["ti"].xcom_pull(
        task_ids="new_file_check"
    )

    if len(file_list)==0:
        print( "No new file founded!!!!!" )


    for filename in file_list: 

            # if filename is None:
            #     print("no file")
            #     return 
       
            print("connection try..") 
            hook = MsSqlHook(
                mssql_conn_id=CONNECTION_ID
            )
        
            sql="select getdate();"
            
            path= CSV_FILE_PATH+'/'+filename

            print(f"connected.. and reading data from csv {path}...")
            
            discarded_row=0

            with open( (path),'r' ) as f:

                print("Header: ",f .readline() )
                cnt=0

                while a:= f.readline():
                    discard_flag=False

                    print("readed line: "+ a + '\n')
            
                    col=a.split(',')

                    if len(col)>45 :
                        discarded_row+=1
                        print("**** Discarded ****")
                        discard_flag=True

                    if cnt==MAX_ROWS_TO_LOAD :
                            break  
                    cnt+=1
                    
                    col_val_list=""
                    for i in range(len(col)):
                        col_val_list+="'" + str(col[i]) + "',"


                    if discard_flag==False:
                        sql= f"insert into banking_data_raw values ( {col_val_list[:-1]} ) ;"
                        print(f"exec-> {sql}")
                    else: 
                        sql=f"insert into discarded_rows(row_value) values ({a});"    
                    hook.run(sql)
                    print("complete insert")     
        
            # sql= "insert into dummy values () ;"
            # # sql= f""" bulk insert dummy from '{ CSV_FILE_PATH+filename }'  with(  format='CSV', firstrow=2, ROWTERMINATOR='0x0A');  """
            # print(sql)   
            # hook.run(sql)
            sql= f"insert into watermark(table_name, file_name)  values ( '{table_name}' , '{filename}' );"
            hook.run(sql) 

            print("exec.. complete for file...")


with DAG(
    dag_id="csv_load",
    start_date=datetime(2026, 9, 11),
    schedule=None,
    catchup=False,
) as dag:

   load_csv_files = PythonOperator(
    python_callable=load_csv_files,
     task_id='load_csv_files'
   )

   start = PythonOperator(
    python_callable=start,
     task_id='start'
   )

   new_file_check = PythonOperator(
    python_callable=new_file_check,
     task_id='new_file_check'
   )
  
   ( start >> new_file_check >> load_csv_files  )