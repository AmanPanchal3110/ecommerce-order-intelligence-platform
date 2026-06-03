

SELECT
    return_reason,
    return_status,
    COUNT(DISTINCT return_id) AS total_returns,
    SUM(refund_amount) AS total_refund_amount,
    COUNT(DISTINCT return_id) * 100.0 /
        NULLIF((SELECT COUNT(DISTINCT order_id) FROM ECOMMERCE.gold.obt), 0) AS return_rate
FROM ECOMMERCE.gold.obt
WHERE return_reason IS NOT NULL
GROUP BY return_reason, return_status
ORDER BY total_returns DESC