
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

WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM ECOMMERCE.bronze.return_bronze
)
