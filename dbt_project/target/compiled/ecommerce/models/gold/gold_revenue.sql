

SELECT
    DATE(event_timestamp) AS revenue_date,
    SUM(price * COALESCE(quantity, 0))  AS gross_revenue,
    SUM(price * COALESCE(quantity, 0)) - COALESCE(SUM(refund_amount), 0) AS net_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(price * COALESCE(quantity, 0)) / NULLIF(COUNT(DISTINCT order_id), 0) AS average_order_value
FROM ECOMMERCE.gold.obt
WHERE current_order_status != 'CANCELLED'
GROUP BY DATE(event_timestamp)
ORDER BY revenue_date