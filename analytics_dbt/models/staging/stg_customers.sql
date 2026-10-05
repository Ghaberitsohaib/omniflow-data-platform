{{ config(materialized='view') }}

WITH source_customers AS (
    SELECT * FROM ecommerce_raw.customers
)

SELECT
    customer_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    email,
    country,
    tier,
    loyalty_points,
    CAST(registered_at AS TIMESTAMP) AS registered_at
FROM source_customers
WHERE customer_id IS NOT NULL
