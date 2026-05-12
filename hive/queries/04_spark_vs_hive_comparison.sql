USE ecommerce_reviews;

SELECT
    review_year,
    review_month,
    COUNT(*) AS review_count,
    ROUND(AVG(rating), 2) AS average_rating
FROM cleaned_reviews
GROUP BY review_year, review_month
ORDER BY review_year, review_month;
