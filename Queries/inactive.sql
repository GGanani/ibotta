SELECT
    DISTINCT CUSTOMER_ID,
    MAX(ACTIVATED) AS last_activation
FROM customer_offers
WHERE CUSTOMER_ID NOT IN (
    SELECT CUSTOMER_ID
    FROM customer_offers
    -- Instead of 'now' use the last activation because data are a few years old 
    WHERE ACTIVATED >= strftime('%s', '2021-03-25 00:00:00', '-2 months')
)
GROUP BY CUSTOMER_ID
ORDER BY last_activation DESC
LIMIT 5;