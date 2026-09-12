{{ config(materialized='view' , 
alias='dim_credit_card') }}

select 
     try_cast( credit_card_id as varchar(13) ) as credit_card_id,
     try_cast( card_type as varchar(25) ) as card_type , 
     try_cast(card_network as varchar(25) ) as card_network , 
     try_cast(credit_limit as float ) as credit_limit , 
     try_cast( card_status as varchar(10)) as card_status 
from banking_data_raw
where credit_card_id is not null 
