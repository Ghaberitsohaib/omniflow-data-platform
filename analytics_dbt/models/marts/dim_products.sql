{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id,
    product_name,
    category,
    cost_price,
    retail_price,
    gross_margin_usd,
    margin_percentage,
    stock_quantity,
    supplier,
    CASE 
        WHEN stock_quantity <= 30 THEN 'Low Stock'
        WHEN stock_quantity <= 100 THEN 'Optimal'
        ELSE 'Surplus'
    END AS inventory_health,
    CASE 
        WHEN retail_price >= 500 THEN 'Premium / Luxury'
        WHEN retail_price >= 100 THEN 'Mid-Range'
        ELSE 'Budget'
    END AS price_tier
FROM products
