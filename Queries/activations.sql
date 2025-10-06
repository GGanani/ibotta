    SELECT
        CUSTOMER_ID as customer_id,
        COUNT(*) AS activation_count
    FROM customer_offers
    GROUP BY customer_id
    ORDER BY activation_count DESC
    LIMIT 5;