{{config(
    materialized = 'incremental',
    unique_key = ['payment_id'],
    incremental_strategy = 'merge'
)}}
SELECT event_id,
    order_id,
    customer_id,
    payment_id,
    payment_method,
    payment_provider,
    card_network,
    issuing_bank,
    amount,
    currency,
    payment_type,
    event_type,
    event_timestamp,
    ingestion_timestamp
FROM {{ source("raw", "raw_payments") }}
{% if is_incremental() %}
WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM {{ this }}
)
{% endif %}