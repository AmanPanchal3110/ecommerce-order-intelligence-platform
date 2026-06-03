
  
    

        create or replace transient table ECOMMERCE.gold.gold_cancel_rate
         as
        (

SELECT
    payment_type,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN current_order_status = 'CANCELLED' THEN order_id END) AS cancelled_orders,
    COUNT(DISTINCT CASE WHEN current_order_status = 'CANCELLED' THEN order_id END) * 100.0
        / NULLIF(COUNT(DISTINCT order_id), 0) AS cancellation_rate
FROM ECOMMERCE.gold.obt
GROUP BY payment_type
ORDER BY payment_type
        );
      
  