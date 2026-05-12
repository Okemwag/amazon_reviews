USE ecommerce_reviews;

CREATE TABLE IF NOT EXISTS gold_review_summary AS
SELECT
    category,
    COUNT(*) AS total_reviews,
    ROUND(AVG(rating), 2) AS average_rating,
    SUM(helpful_vote) AS helpful_votes
FROM cleaned_reviews
GROUP BY category;
