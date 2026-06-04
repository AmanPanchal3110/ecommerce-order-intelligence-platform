
WITH silver_amount AS (
    SELECT 
        order_id, 
        MAX(total_amount) AS total_amount
    FROM ECOMMERCE.bronze.items_bronze
    GROUP BY order_id
),
payments AS (
            SELECT event_id,
            order_id,
            customer_id,
            payment_id,
            CASE
            WHEN payment_method IS NULL THEN
                CASE
                    WHEN payment_provider IN ('GPay', 'PhonePe', 'Paytm') THEN 'UPI'
                    WHEN payment_provider IN ('HDFC', 'ICICI', 'SBI')      THEN 'NETBANKING'
                    ELSE 'CARD'
                END
                ELSE payment_method
            END AS payment_method,
            payment_provider,
            card_network,
            issuing_bank,
            currency,
            payment_type,
            event_type,
            LEAST(TO_TIMESTAMP_NTZ(event_timestamp), TO_TIMESTAMP_NTZ(ingestion_timestamp)) AS event_timestamp,
            ingestion_timestamp
        FROM ECOMMERCE.bronze.payment_bronze
        
        WHERE INGESTION_TIMESTAMP > (
            SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '2000-01-01')
            FROM ECOMMERCE.silver.payment_silver
        )
        
)
SELECT
    p.event_id,
    p.order_id,
    p.customer_id,
    p.payment_id,
    p.payment_method,
    p.payment_provider,
    p.card_network,
    p.issuing_bank,
    s.total_amount AS amount,
    p.currency,
    p.payment_type,
    p.event_type,
    p.event_timestamp,
    p.ingestion_timestamp
FROM payments p
LEFT JOIN silver_amount s
    ON p.order_id = s.order_id