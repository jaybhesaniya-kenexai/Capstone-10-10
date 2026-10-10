{{ config(
    materialized='incremental',
    unique_key=['match_id'],
    alias='dim_match',
    tags=['silver']
    ) }}
with cte as( 
select distinct
     match_id,
     match_format,
     tournament_name,
     season,
     match_stage,
     toss_decision,
     getdate() as insert_date
from airflow_test.dbo.cricket_clean
 )
 SELECT distinct match_id,
     match_format ,
     first_value(tournament_name) over(partition by match_id,match_format order by tournament_name desc ) as  tournament_name,
     season,
     match_stage,
     toss_decision,
     getdate() as insert_date
from cte     