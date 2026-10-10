{{ config(
    materialized='incremental',
    alias='dim_team',
    unique_key=['team_id'],
    tags=['silver']
    ) }}

with cte as(
SELECT  
    batting_team_id as  team_id,
    batting_team_name  as team_name
from airflow_test.dbo.cricket_clean 
union 

SELECT  
    bowling_team_id as team_id,
    bowling_team_name as team_name
from airflow_test.dbo.cricket_clean
)
select distinct team_id , team_name
from cte ;