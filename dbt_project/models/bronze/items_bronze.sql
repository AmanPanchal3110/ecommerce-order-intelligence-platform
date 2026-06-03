{{config(
    materialized = 'incremental',
    unique_key = ['order_id', 'product_id'],
    incremental_strategy = 'merge'
)}}
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
FROM {{source("raw","raw_order_items")}}
{% if is_incremental() %}
WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM {{ this }}
)
{% endif %}