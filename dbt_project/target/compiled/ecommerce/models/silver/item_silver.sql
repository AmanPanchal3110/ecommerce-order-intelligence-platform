
SELECT event_id,
    order_id,
    customer_id,
    customer_name,
    customer_email,
    product_id,
    CASE WHEN price < 0 THEN price * -1 ELSE price END AS price,
    CASE WHEN quantity <= 0 THEN NULL ELSE quantity END AS quantity,
    CASE WHEN quantity <= 0 THEN TRUE ELSE FALSE END AS is_quantity_corrupted,
    total_amount,
    payment_type,
    currency,
    LEAST(TO_TIMESTAMP_NTZ(event_timestamp), TO_TIMESTAMP_NTZ(ingestion_timestamp)) AS event_timestamp,
    ingestion_timestamp
FROM ECOMMERCE.bronze.items_bronze

WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM ECOMMERCE.silver.item_silver
)
