SELECT
    co.customer_id as customer_id,
    SUM(cor.OFFER_AMOUNT * cor.VERIFIED_REDEMPTION_COUNT) AS total_redeemed
FROM
    customer_offer_redemptions cor
JOIN
    customer_offers co ON cor.CUSTOMER_OFFER_ID = co.ID
GROUP BY co.customer_id
ORDER BY total_redeemed DESC
LIMIT 5;