
SELECT event_id,
    order_id,
    customer_id,
    customer_name,
    customer_email,
    product_id,
    quantity,
    price,
    total_amount,
    payment_type,
    currency,
    event_timestamp,
    ingestion_timestamp
FROM ecommerce.RAW.raw_order_items

WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM ECOMMERCE.bronze.items_bronze
)
