# HDFS Structure

The local Docker pipeline uses these HDFS zones:

```text
/user/bigdata/amazon_reviews/bronze/office_products/
/user/bigdata/amazon_reviews/silver/office_products_cleaned/
/user/bigdata/amazon_reviews/gold/
/user/bigdata/amazon_reviews/quarantine/invalid_reviews/
/user/bigdata/amazon_reviews/models/
/user/bigdata/amazon_reviews/metrics/
```

Legacy compatibility paths used by the existing Hive DDL are also maintained:

```text
/user/bigdata/landing/amazon_reviews/reviews/
/user/bigdata/processed/amazon_reviews/reviews_cleaned/
/user/bigdata/manifests/amazon_reviews/
```
