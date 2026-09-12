{{ config(materialized='view' , 
alias='dim_account') }}
select top 2
     try_cast( account_id as varchar(13) ) as account_id,
     try_cast( account_type as varchar(20) ) as account_type , 
     try_cast(account_open_date as date) as account_open_date , 
     try_cast(account_status as varchar(2) ) as account_status , 
     try_cast(branch_id as varchar(50) ) as branch_id 
from banking_data_raw
where account_id is not null ;