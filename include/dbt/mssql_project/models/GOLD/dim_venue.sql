{{ config(
    materialized='view',
    alias='DIM_VENUE'
    ) }}

SELECT distinct
    cast( upper(trim(venue_id)) as varchar) as venue_id,
    cast( upper(trim(venue_name)) as varchar) as venue_name,
    cast( upper(trim(venue_city)) as varchar ) as venue_city,
	cast( upper(trim(venue_country)) as varchar ) as venue_country
from bronze_cricket  ;