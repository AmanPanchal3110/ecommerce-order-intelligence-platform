
  create or replace   view ECOMMERCE.dbt.demo
  
   as (
    SELECT * 
FROM ecommerce.RAW.orders
  );

