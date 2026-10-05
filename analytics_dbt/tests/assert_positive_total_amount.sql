-- Custom dbt singular test: Ensures no order has negative or zero total amount
SELECT
    order_id,
    total_amount_usd
FROM {{ ref('stg_orders') }}
WHERE total_amount_usd <= 0
