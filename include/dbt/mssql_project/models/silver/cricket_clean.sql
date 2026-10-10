{{ config(
    materialized='incremental',
    unique_key=['delivery_id'],
    alias='cricket_clean'
    ) }}

SELECT    
    upper(trim(delivery_id)) as delivery_id ,
    try_cast( delivery_timestamp as  datetime ) as delivery_timestamp,
    try_cast( innings_no as int) as innings_no,
    try_cast( over_no as int) as over_no ,
    try_cast( ball_in_over as int) as  ball_in_over ,
    try_cast( runs_off_bat as int) as runs_off_bat,
    upper(trim(extra_type)) as extra_type ,
    try_cast( extra_runs as int) as extra_runs,
    try_cast( total_runs as int) as total_runs,
    try_cast( is_wicket as bit ) as is_wicket,
    upper(trim(dismissal_type)) as dismissal_type ,
    try_cast( ball_speed_kmph as float) as ball_speed_kmph,
    upper(trim(non_striker_id)) as non_striker_id ,
    upper(trim(shot_type)) as shot_type ,
    upper(trim(shot_region)) as shot_region ,
    upper(trim(match_id)) as match_id ,
    try_convert(date, match_date ) as match_date,
    upper(trim(match_format)) as match_format,
    upper(trim(tournament_name)) as tournament_name,
    try_cast( season as int) as season,
    upper(trim(match_stage))as match_stage ,
    upper(trim(toss_decision)) toss_decision ,
    upper(trim(venue_id )) as venue_id,
    upper(trim(venue_name)) as venue_name,
    upper(trim(venue_city)) as venue_city ,
    upper(trim(venue_country)) as venue_country ,
    upper(trim(batting_team_id)) as batting_team_id ,
    upper(trim(batting_team_name)) as batting_team_name ,
    upper(trim(bowling_team_id)) as bowling_team_id ,
    upper(trim(bowling_team_name)) as bowling_team_name ,
    upper(trim(batsman_id)) as batsman_id ,
    upper(trim(batsman_name)) as batsman_name ,
    try_convert(date, batsman_dob ) as batsman_dob,
    upper(trim(batsman_nationality)) as batsman_nationality ,
    upper(trim(batting_hand)) as batting_hand,
    upper(trim(batsman_role)) as batsman_role ,
    
    case when  batsman_email  like '%_@_%._%'
       then  upper(trim(batsman_email)) 
    else NULL
     end  as batsman_email ,
    
    try_cast( batsman_phone as int) as batsman_phone ,
    try_cast( batsman_icc_rating as int ) as batsman_icc_rating,
    upper(trim(bowler_id)) as bowler_id ,
    upper(trim(bowler_name)) as bowler_name ,
    upper(trim(bowler_nationality)) as bowler_nationality ,
    upper(trim(bowling_style)) as bowling_style ,
    try_cast( bowler_icc_rating as int ) as bowler_icc_rating,
    upper(trim(contract_id )) as contract_id ,
    upper(trim(contract_grade)) as contract_grade ,
    upper(trim(annual_retainer_usd)) as annual_retainer_usd,
    upper(trim(contract_status)) as contract_status ,
    upper(trim(drs_review_id)) as drs_review_id ,
    upper(trim(drs_reviewed_by)) as drs_reviewed_by ,
    upper(trim(drs_original_decision)) as drs_original_decision,
    upper(trim(drs_outcome)) as drs_outcome ,

    getdate() as insert_date
from airflow_test.dbo.bronze_cricket ;