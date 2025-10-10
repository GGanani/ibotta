-- Conversion rate of offered → verified per customer
WITH customer_activation_stats AS (
    SELECT 
        CUSTOMER_ID as customer_id,
        COUNT(*) AS total_offered, -- let's count all offers whether or not activated
        SUM(CASE WHEN VERIFIED IS NOT NULL THEN 1 ELSE 0 END) AS total_verified
    FROM customer_offers
    GROUP BY customer_id
)
SELECT 
    customer_id,
    total_offered,
    total_verified,
    ROUND(CAST(total_verified AS REAL) / total_offered, 2) AS conversion_rate
FROM customer_activation_stats
;