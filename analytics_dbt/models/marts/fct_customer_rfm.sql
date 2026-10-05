{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        customer_id,
        MAX(created_at) AS last_purchase_at,
        COUNT(order_id) AS frequency,
        ROUND(SUM(total_amount_usd), 2) AS monetary_value
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),

reference_date AS (
    SELECT MAX(created_at) AS max_date FROM {{ ref('stg_orders') }}
)

SELECT
    c.customer_id,
    c.frequency,
    c.monetary_value,
    DATE_DIFF('day', c.last_purchase_at, r.max_date) AS recency_days,
    CASE
        WHEN c.frequency >= 3 AND c.monetary_value >= 1200 THEN 'Champions'
        WHEN c.frequency >= 2 AND c.monetary_value >= 500 THEN 'Loyal Customers'
        WHEN DATE_DIFF('day', c.last_purchase_at, r.max_date) <= 3 THEN 'New Active'
        WHEN c.monetary_value >= 800 THEN 'High Value'
        ELSE 'Standard'
    END AS rfm_segment,
    CURRENT_TIMESTAMP AS snapshot_timestamp
FROM customer_orders c
CROSS JOIN reference_date r
