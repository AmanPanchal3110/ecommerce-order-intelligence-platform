

WITH STATUS_TABLE AS (
    SELECT 
        order_id,
        MAX(CASE WHEN ORDER_STATUS = 'CREATED' THEN STATUS_TIMESTAMP END) AS created_at,
        MAX(CASE WHEN ORDER_STATUS = 'CONFIRMED' THEN STATUS_TIMESTAMP END) AS confirmed_at,
        MAX(CASE WHEN ORDER_STATUS = 'SHIPPED' THEN STATUS_TIMESTAMP END) AS shipped_at,
        MAX(CASE WHEN ORDER_STATUS = 'DELIVERED' THEN STATUS_TIMESTAMP END) AS delivered_at,
        MAX(CASE WHEN ORDER_STATUS = 'CANCELLED' THEN STATUS_TIMESTAMP END) AS cancelled_at,
        CASE MAX(CASE 
                WHEN order_status = 'DELIVERED' THEN 4
                WHEN order_status = 'SHIPPED' THEN 3
                WHEN order_status = 'CONFIRMED' THEN 2
                WHEN order_status = 'CANCELLED' THEN 1
                WHEN order_status = 'CREATED' THEN 0
            END)
                WHEN 4 THEN 'DELIVERED'
                WHEN 3 THEN 'SHIPPED'
                WHEN 2 THEN 'CONFIRMED'
                WHEN 1 THEN 'CANCELLED'
                WHEN 0 THEN 'CREATED'
            END AS current_order_status,
        CASE MAX(CASE
            WHEN payment_status = 'SUCCESS' THEN 2
            WHEN payment_status = 'FAILED' THEN 1
            WHEN payment_status = 'PENDING' THEN 0
        END)
            WHEN 2 THEN 'SUCCESS'
            WHEN 1 THEN 'FAILED'
            WHEN 0 THEN 'PENDING'
        END AS current_payment_status
    FROM ECOMMERCE.silver.status_silver
    GROUP BY order_id
)
SELECT
    i.order_id,
    i.product_id,
    i.customer_id,
    i.customer_name,
    i.customer_email,
    i.price,
    i.quantity,
    i.is_quantity_corrupted,
    i.total_amount,
    i.payment_type,
    p.payment_method,
    p.payment_provider,
    p.card_network,
    p.issuing_bank,
    st.current_order_status,
    st.current_payment_status,
    st.created_at,
    st.confirmed_at,
    st.shipped_at,
    st.delivered_at,
    st.cancelled_at,
    r.return_id,
    r.return_reason,
    r.return_status,
    r.refund_amount,
    i.currency,
    i.event_timestamp
FROM ECOMMERCE.silver.item_silver i
LEFT JOIN ECOMMERCE.silver.payment_silver p ON i.order_id = p.order_id
LEFT JOIN STATUS_TABLE st ON i.order_id = st.order_id 
LEFT JOIN ECOMMERCE.silver.return_silver r ON i.order_id = r.order_id