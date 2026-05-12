USE ecommerce_reviews;

SELECT COUNT(*) AS invalid_rating_count
FROM raw_reviews
WHERE rating < 1 OR rating > 5;

SELECT
    SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) AS missing_rating,
    SUM(CASE WHEN text IS NULL THEN 1 ELSE 0 END) AS missing_text,
    SUM(CASE WHEN asin IS NULL THEN 1 ELSE 0 END) AS missing_asin,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS missing_user_id
FROM cleaned_reviews;
