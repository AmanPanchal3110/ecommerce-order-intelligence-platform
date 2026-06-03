{{config(
    materialized = 'incremental',
    unique_key = ['return_id'],
    incremental_strategy = 'merge'
)}}
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
FROM {{ source("raw", "raw_returns") }}
{% if is_incremental() %}
WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM {{ this }}
)
{% endif %}