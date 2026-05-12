from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, month, to_date, when, year
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType


CLEAN_TABLE = "ecommerce_reviews.cleaned_reviews"
RAW_PATH = "hdfs://namenode:9000/user/bigdata/landing/amazon_reviews/reviews"
OUTPUT_PATH = "hdfs://namenode:9000/user/bigdata/processed/amazon_reviews/reviews_cleaned"
RAW_SCHEMA = StructType(
    [
        StructField("rating", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("helpful_vote", LongType(), True),
        StructField("verified_purchase", BooleanType(), True),
        StructField("category", StringType(), True),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("AmazonReviewsTransform")
        .enableHiveSupport()
        .getOrCreate()
    )

    raw_df = spark.read.schema(RAW_SCHEMA).json(RAW_PATH)

    clean_df = (
        raw_df.dropna(subset=["rating", "text", "asin", "user_id"])
        .filter((col("rating") >= 1) & (col("rating") <= 5))
        .withColumn("sentiment_label", when(col("rating") >= 4, 1).otherwise(0))
        .withColumn("review_date", to_date(from_unixtime(col("timestamp") / 1000)))
        .withColumn("review_year", year(col("review_date")))
        .withColumn("review_month", month(col("review_date")))
        .withColumnRenamed("timestamp", "review_ts")
        .select(
            "rating",
            "title",
            "text",
            "asin",
            "parent_asin",
            "user_id",
            "review_ts",
            "helpful_vote",
            "verified_purchase",
            "category",
            "sentiment_label",
            col("review_date").cast("string").alias("review_date"),
            "review_year",
            "review_month",
        )
    )

    clean_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.sql(f"REFRESH TABLE {CLEAN_TABLE}")

    print("Cleaned review counts by sentiment label:")
    clean_df.groupBy("sentiment_label").count().show()
    print(f"Wrote cleaned reviews to {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
