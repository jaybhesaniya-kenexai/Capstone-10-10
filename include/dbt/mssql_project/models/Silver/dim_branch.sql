{{ config(materialized='view' , 
alias='dim_branch') }}
select top 2
     try_cast( branch_id as varchar(7) ) as branch_id,
     try_cast( branch_name as varchar(100) ) as branch_name 
from banking_data_raw
where account_id is not null ;