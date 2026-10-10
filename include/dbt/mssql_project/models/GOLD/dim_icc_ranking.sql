{{ config(
    materialized='incremental',
    alias='dim_icc_ranking',
    unique_key=['player_id','ranking_role','icc_ranking'],
    tags=['silver']
    ) }}

with cte as 
(SELECT 
    batsman_id as player_id,
    'BATSMAN' as ranking_role,
    batsman_icc_rating as icc_ranking ,
    delivery_timestamp
from airflow_test.dbo.cricket_clean  
union 
select 
    bowler_id as player_id,
    'BOWLER' as ranking_role,
    bowler_icc_rating   as icc_ranking ,
    delivery_timestamp
from  airflow_test.dbo.cricket_clean 
	),
latest_ranking(    
select     
    player_id, 
   ranking_role,
   first_value(icc_ranking) over(partition by player_id,ranking_role order by delivery_timestamp desc) as icc_ranking
from cte 
),   
select distinct
   player_id, 
   ranking_role,
   icc_ranking ,
  getdate() as insert_date
from  latest_ranking