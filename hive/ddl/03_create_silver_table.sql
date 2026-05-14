USE ecommerce_reviews;

DROP TABLE IF EXISTS cleaned_reviews;

CREATE EXTERNAL TABLE cleaned_reviews (
    rating DOUBLE,
    title STRING,
    text STRING,
    asin STRING,
    parent_asin STRING,
    user_id STRING,
    review_ts BIGINT,
    helpful_vote BIGINT,
    verified_purchase BOOLEAN,
    category STRING,
    sentiment_label INT,
    rating_class STRING,
    review_length INT,
    word_count INT,
    helpful_vote_bucket STRING,
    verified_purchase_int INT,
    review_date STRING
)
PARTITIONED BY (review_year INT, review_month INT)
STORED AS PARQUET
LOCATION '/user/bigdata/processed/amazon_reviews/reviews_cleaned';

MSCK REPAIR TABLE cleaned_reviews;
