-- Conversion rate of activated → verified per customer
WITH customer_activation_stats AS (
    SELECT 
        CUSTOMER_ID,
        COUNT(*) AS total_activated,
        SUM(CASE WHEN VERIFIED IS NOT NULL THEN 1 ELSE 0 END) AS total_verified
    FROM customer_offers
    GROUP BY CUSTOMER_ID
)
SELECT 
    CUSTOMER_ID,
    total_activated,
    total_verified,
    ROUND(CAST(total_verified AS REAL) / total_activated, 2) AS conversion_rate
FROM customer_activation_stats
LIMIT 5;