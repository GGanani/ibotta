SELECT DISTINCT CUSTOMER_ID
FROM customer_offers
WHERE CUSTOMER_ID NOT IN (
    SELECT CUSTOMER_ID
    FROM customer_offers
    WHERE ACTIVATED >= strftime('%s', 'now', '-2 months')
)
LIMIT 5;