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
    LEAST(TO_TIMESTAMP_NTZ(event_timestamp), TO_TIMESTAMP_NTZ(ingestion_timestamp)) AS event_timestamp,
    ingestion_timestamp
FROM {{ref('return_bronze') }}
{% if is_incremental() %}
WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM {{ this }}
)
{% endif %}