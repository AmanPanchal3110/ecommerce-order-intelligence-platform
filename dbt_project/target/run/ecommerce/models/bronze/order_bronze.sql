
  
    

        create or replace transient table ECOMMERCE.dbt.order_bronze
         as
        (
SELECT * 
FROM ecommerce.RAW.orders

        );
      
  