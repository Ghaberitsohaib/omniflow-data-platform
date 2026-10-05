{{ config(materialized='view') }}

WITH source_products AS (
    SELECT * FROM ecommerce_raw.products
)

SELECT
    product_id,
    name AS product_name,
    category,
    cost_price,
    retail_price,
    ROUND(retail_price - cost_price, 2) AS gross_margin_usd,
    ROUND(((retail_price - cost_price) / retail_price) * 100, 1) AS margin_percentage,
    stock_quantity,
    supplier
FROM source_products
WHERE product_id IS NOT NULL
