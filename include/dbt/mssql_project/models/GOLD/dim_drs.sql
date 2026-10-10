{{ config(
    materialized='incremental',
    alias='dim_drs',
    unique_key=['drs_review_id'],
    tags=['silver']
    ) }}

SELECT distinct
    coalesce(drs_review_id, 'NULL') as drs_review_id,
    coalesce(drs_reviewed_by, 'NULL') as drs_reviewed_by,
    coalesce(drs_original_decision, 'NULL') as drs_original_decision,
	coalesce(drs_outcome, 'NULL') as drs_outcome,
    getdate() as insert_date
from airflow_test.dbo.cricket_clean ;
