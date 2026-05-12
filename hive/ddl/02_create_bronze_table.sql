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
