USE ecommerce_reviews;

SELECT
    sentiment_label,
    COUNT(*) AS total_reviews
FROM cleaned_reviews
GROUP BY sentiment_label
ORDER BY sentiment_label;

SELECT
    verified_purchase,
    ROUND(AVG(rating), 2) AS average_rating,
    COUNT(*) AS review_count
FROM cleaned_reviews
GROUP BY verified_purchase
ORDER BY review_count DESC;

SELECT
    category,
    ROUND(AVG(rating), 2) AS average_rating,
    COUNT(*) AS total_reviews,
    SUM(helpful_vote) AS helpful_votes
FROM cleaned_reviews
GROUP BY category
ORDER BY total_reviews DESC
LIMIT 10;

SELECT
    asin,
    ROUND(AVG(rating), 2) AS average_rating,
    COUNT(*) AS total_reviews,
    SUM(helpful_vote) AS helpful_votes
FROM cleaned_reviews
GROUP BY asin
ORDER BY total_reviews DESC, helpful_votes DESC
LIMIT 10;
