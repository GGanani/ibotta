SELECT
    DISTINCT customer_id as customer_id,
    MAX(ACTIVATED) AS last_activation
FROM customer_offers
WHERE customer_id NOT IN (
    SELECT customer_id
    FROM customer_offers
    -- Instead of 'now' use the last activation because data are a few years old 
    WHERE date(ACTIVATED) >= date('2021-03-25', '-2 months')
)
GROUP BY customer_id
ORDER BY last_activation DESC
;