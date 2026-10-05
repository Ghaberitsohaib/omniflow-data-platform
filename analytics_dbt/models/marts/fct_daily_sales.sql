{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    order_date,
    country,
    payment_method,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(subtotal_usd), 2) AS gross_sales,
    ROUND(SUM(discount_usd), 2) AS total_discounts,
    ROUND(SUM(tax_usd), 2) AS total_tax,
    ROUND(SUM(shipping_usd), 2) AS total_shipping,
    ROUND(SUM(total_amount_usd), 2) AS net_revenue,
    ROUND(AVG(total_amount_usd), 2) AS avg_order_value,
    SUM(CASE WHEN is_fraud_suspect = true THEN 1 ELSE 0 END) AS flagged_fraud_orders
FROM orders
GROUP BY order_date, country, payment_method
ORDER BY order_date DESC, net_revenue DESC
