{{ config(
    materialized='incremental',
    alias='DIM_VENUE',
    unique_key=['venue_id'],
    tags=['silver']
    ) }}

SELECT distinct
    venue_id,
    venue_name,
    venue_city,
	venue_country,
    getdate() as insert_date
from airflow_test.dbo.cricket_clean  ;