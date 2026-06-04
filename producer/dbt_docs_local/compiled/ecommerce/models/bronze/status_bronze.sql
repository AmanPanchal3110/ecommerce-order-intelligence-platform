

SELECT
    event_id,
    order_id,
    customer_id,
    event_timestamp,
    ingestion_timestamp,
    order_status,
    payment_status,
    status_timestamp
FROM ecommerce.RAW.raw_order_status


WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM ECOMMERCE.bronze.status_bronze
)
