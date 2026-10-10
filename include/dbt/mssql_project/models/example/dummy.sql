{{ config(
    materialized='table'
    ) }}

SELECT
    1 AS id,
    'Test' AS name,
    GETDATE() AS created_at;