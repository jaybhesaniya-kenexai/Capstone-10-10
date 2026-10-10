{{ config(
    materialized='incremental',
    alias='fact_delivery',
    unique_key=['delivery_id'],
    tags=['gold']
    ) }}

select 
  delivery_id,
  delivery_timestamp,
  innings_no,
  over_no,
  ball_in_over,
  runs_off_bat,
  extra_type,
  extra_runs,
  total_runs,
  is_wicket,
  dismissal_type,
  ball_speed_kmph,
  batsman_id as striker_batsman_id,
  non_striker_id,
  shot_type,
  shot_region,

  match_id,
  bowler_id,
  drs_review_id,
  venue_id,
  batting_team_id,
  bowling_team_id,
  
  getdate() as insert_date
from airflow_test.dbo.cricket_clean;