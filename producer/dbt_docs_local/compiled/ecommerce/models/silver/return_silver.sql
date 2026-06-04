
SELECT event_id,
    order_id,
    customer_id,
    return_id,
    return_reason,
    return_status,
    refund_amount,
    currency,
    event_type,
    LEAST(TO_TIMESTAMP_NTZ(event_timestamp), TO_TIMESTAMP_NTZ(ingestion_timestamp)) AS event_timestamp,
    ingestion_timestamp
FROM ECOMMERCE.bronze.return_bronze

WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM ECOMMERCE.silver.return_silver
)
