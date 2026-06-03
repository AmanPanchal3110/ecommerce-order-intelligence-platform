
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
FROM ecommerce.RAW.raw_payments
