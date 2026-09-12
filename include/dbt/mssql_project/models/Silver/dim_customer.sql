{{ config(materialized='view' , 
alias='dim_customer') }}
select top 2
     try_cast( customer_id as varchar(13) ) as Customer_id,
     try_cast( customer_name as varchar(40) ) as name , 
     try_cast(customer_dob as date) as date_of_birth , 
     try_cast(customer_gender as varchar(2) ) as gender , 
     try_cast(customer_email as varchar(50) ) as email , 
     try_cast(customer_phone as int ) as phone , 
     try_cast(customer_city as varchar(40) ) as city , 
     try_cast(customer_country as varchar(30) ) as country ,
     try_cast(customer_segment as  varchar(20) ) as segment ,
     try_cast(kyc_status as varchar(20) ) as kyc_status,
     try_cast(customer_annual_income as float ) as annual_income
from banking_data_raw
where customer_id is not null ;