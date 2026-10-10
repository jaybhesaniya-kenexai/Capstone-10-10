{{ config(
    materialized='incremental',
    alias='dim_player',
    unique_key=['player_id'],
    tags=['silver']
    ) }}


with batsman as(
select 
   batsman_id as player_id,
   batsman_name as player_name,
   batsman_dob as player_dob,
   batsman_nationality as nationality,
   batsman_email as email,
   batting_hand ,
   batsman_role ,
   batsman_phone as phone,
   NULL as bowling_style
from airflow_test.dbo.cricket_clean
),
bowler as(
select 
    bowler_id as player_id,
    bowler_name as player_name,
    NULL as player_dob ,
    bowler_nationality as nationality,
    NULL as email,
    NULL as batting_hand,
    null as batsman_role ,
    NULL as phone ,    
    bowling_style  
from airflow_test.dbo.cricket_clean
)
 select distinct
   coalesce(bt.player_id, bw.player_id ) as player_id,
   coalesce(bt.player_name, bw.player_name) as player_name,
   coalesce(bt.player_dob , bw.player_dob  ) as player_dob,
   coalesce(bt.nationality , bw.nationality  ) as nationality,
   coalesce(bt.email, bw.email ) as email,
   coalesce(bt.batting_hand , bw.batting_hand ) as batting_hand,
   coalesce(bt.batsman_role , bw.batsman_role ) as batsman_role,
   coalesce(bt.phone , bw.phone ) as phone,
   coalesce(bt.bowling_style , bw.bowling_style ) as bowling_style
from batsman bt left join bowler bw
on bt.player_id=bw.player_id;

