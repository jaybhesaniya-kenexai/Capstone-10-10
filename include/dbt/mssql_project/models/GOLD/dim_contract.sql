{{ config(
    materialized='incremental',
    alias='dim_contract',
    unique_key=['player_id','contract_id'],
    tags=['silver']
    ) }}

with cte as(
SELECT 
 coalesce(contract_id,'NULL' ) as contract_id ,
 batsman_id as player_id ,
 first_value(contract_grade) over(partition by contract_id,batsman_id order by delivery_timestamp desc) as contract_grade,
 first_value(annual_retainer_usd) over(partition by contract_id,batsman_id order by delivery_timestamp desc) as annual_retainer_usd,
 first_value(contract_status) over(partition by contract_id,batsman_id order by delivery_timestamp desc) as contract_status
from airflow_test.dbo.cricket_clean  
)

SELECT contract_id ,
 contract_grade,
 batsman_id as player_id ,
 annual_retainer_usd,
 contract_status   ,
 getdate() as insert_date
from airflow_test.dbo.cricket_clean  ;