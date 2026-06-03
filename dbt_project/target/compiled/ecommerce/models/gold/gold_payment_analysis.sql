

SELECT
    payment_type,
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(price * COALESCE(quantity, 0)) AS gross_revenue,
    SUM(price * COALESCE(quantity, 0)) - COALESCE(SUM(refund_amount), 0) AS net_revenue
FROM ECOMMERCE.gold.obt
GROUP BY payment_type, payment_method
ORDER BY payment_type, payment_method