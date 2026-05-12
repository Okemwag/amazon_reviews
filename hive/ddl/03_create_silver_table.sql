USE ecommerce_reviews;

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned_reviews (
    rating DOUBLE,
    title STRING,
    text STRING,
    asin STRING,
    parent_asin STRING,
    user_id STRING,
    review_ts BIGINT,
    helpful_vote INT,
    verified_purchase BOOLEAN,
    category STRING,
    sentiment_label INT,
    review_date STRING,
    review_year INT,
    review_month INT
)
STORED AS PARQUET
LOCATION '/user/bigdata/processed/amazon_reviews/reviews_cleaned';
