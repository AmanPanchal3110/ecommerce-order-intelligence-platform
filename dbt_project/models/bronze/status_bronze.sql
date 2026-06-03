{{ config(
    materialized = 'incremental',
    unique_key = ['order_id', 'order_status','status_timestamp'],
    incremental_strategy = 'merge'
) }}

SELECT
    event_id,
    order_id,
    customer_id,
    event_timestamp,
    ingestion_timestamp,
    order_status,
    payment_status,
    status_timestamp
FROM {{ source("raw", "raw_order_status") }}

{% if is_incremental() %}
WHERE INGESTION_TIMESTAMP > (
    SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
    FROM {{ this }}
)
{% endif %}