CREATE DATABASE IF NOT EXISTS ecommerce_reviews;

USE ecommerce_reviews;

DROP TABLE IF EXISTS raw_reviews;

CREATE EXTERNAL TABLE raw_reviews (
    rating DOUBLE,
    title STRING,
    text STRING,
    asin STRING,
    parent_asin STRING,
    user_id STRING,
    `timestamp` BIGINT,
    helpful_vote INT,
    verified_purchase BOOLEAN,
    category STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/bigdata/landing/amazon_reviews/reviews';

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

SHOW TABLES;
