

SELECT
    event_id,
    order_id,
    customer_id,
    LEAST(TO_TIMESTAMP_NTZ(event_timestamp), TO_TIMESTAMP_NTZ(ingestion_timestamp)) AS event_timestamp,
    ingestion_timestamp,
    order_status,
    payment_status,
    status_timestamp
FROM ECOMMERCE.bronze.status_bronze


WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM ECOMMERCE.silver.status_silver
)
