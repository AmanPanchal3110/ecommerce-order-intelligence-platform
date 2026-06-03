
  
    

        create or replace transient table ECOMMERCE.dbt.return_bronze
         as
        (
SELECT event_id,
    order_id,
    customer_id,
    return_id,
    return_reason,
    return_status,
    refund_amount,
    currency,
    event_type,
    event_timestamp,
    ingestion_timestamp
FROM ecommerce.RAW.raw_returns

        );
      
  