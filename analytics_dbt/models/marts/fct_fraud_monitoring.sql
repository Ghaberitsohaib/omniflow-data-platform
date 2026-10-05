{{ config(materialized='table') }}

WITH flagged_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE is_fraud_suspect = true OR total_amount_usd >= 2000
)

SELECT
    order_id,
    customer_id,
    total_amount_usd,
    payment_method,
    country,
    city,
    ip_address,
    device,
    COALESCE(fraud_reason, 'HIGH_VALUE_THRESHOLD') AS fraud_reason,
    CASE 
        WHEN total_amount_usd >= 4000 THEN 'CRITICAL'
        WHEN total_amount_usd >= 2000 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS risk_severity,
    created_at AS incident_time
FROM flagged_orders
ORDER BY total_amount_usd DESC
