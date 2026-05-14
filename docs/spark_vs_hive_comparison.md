# Spark vs Hive Comparison

Run the benchmark job after the silver table exists:

```bash
docker exec amazon_reviews_spark_master bash -c '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/amazon_reviews_pipeline/analytics/spark_vs_hive_benchmark.py'
```

The benchmark writes:

```text
output/metrics/spark_vs_hive_benchmark.json
```

Tasks compared:

- Total record count.
- Group by rating.
- Group by verified purchase.
- Group by review year/month.

The goal is not to prove Spark is always faster. The goal is to show that the
same analytical questions can be expressed through Spark DataFrames and SQL,
then measured consistently.
