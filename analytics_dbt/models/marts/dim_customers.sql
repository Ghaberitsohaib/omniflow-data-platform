{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_aggregates AS (
    SELECT
        customer_id,
        COUNT(order_id) AS lifetime_orders,
        ROUND(SUM(total_amount_usd), 2) AS lifetime_spend_usd,
        ROUND(AVG(total_amount_usd), 2) AS avg_spend_per_order,
        MIN(created_at) AS first_order_date,
        MAX(created_at) AS last_order_date
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.country,
    c.tier,
    c.loyalty_points,
    COALESCE(o.lifetime_orders, 0) AS lifetime_orders,
    COALESCE(o.lifetime_spend_usd, 0.0) AS lifetime_spend_usd,
    COALESCE(o.avg_spend_per_order, 0.0) AS avg_spend_per_order,
    o.first_order_date,
    o.last_order_date,
    CASE 
        WHEN COALESCE(o.lifetime_spend_usd, 0) >= 1500 THEN 'VIP Tier'
        WHEN COALESCE(o.lifetime_spend_usd, 0) >= 500 THEN 'High Value'
        WHEN COALESCE(o.lifetime_orders, 0) >= 1 THEN 'Active Buyer'
        ELSE 'Prospect'
    END AS customer_lifecycle_stage
FROM customers c
LEFT JOIN order_aggregates o ON c.customer_id = o.customer_id
