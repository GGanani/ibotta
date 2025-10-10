    SELECT
        CUSTOMER_ID as customer_id,
        SUM(CASE WHEN activated IS NOT NULL THEN 1 ELSE 0 END) AS activation_count
    FROM customer_offers
    GROUP BY customer_id
    ORDER BY activation_count DESC
    ;