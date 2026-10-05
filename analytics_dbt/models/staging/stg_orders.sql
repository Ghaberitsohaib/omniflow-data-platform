{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM raw_orders
)

SELECT
    order_id,
    customer_id,
    item_count,
    COALESCE(primary_category, 'General') AS primary_category,
    ROUND(subtotal, 2) AS subtotal_usd,
    ROUND(discount, 2) AS discount_usd,
    ROUND(tax, 2) AS tax_usd,
    ROUND(shipping, 2) AS shipping_usd,
    ROUND(total_amount, 2) AS total_amount_usd,
    LOWER(payment_method) AS payment_method,
    currency,
    city,
    country,
    device,
    ip_address,
    is_fraud_suspect,
    fraud_reason,
    order_status,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(created_at AS DATE) AS order_date
FROM source_data
WHERE order_id IS NOT NULL
