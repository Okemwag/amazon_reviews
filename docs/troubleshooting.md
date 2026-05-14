# Troubleshooting

## `ModuleNotFoundError: No module named 'numpy'`

Spark MLlib imports `numpy` inside the Spark container. Rebuild the Spark image:

```bash
docker compose build spark-master spark-worker
docker compose up -d --no-deps --force-recreate spark-master spark-worker
```

## `PermissionError` Writing `/workspace/output`

The pipeline now normalizes `/workspace/output` permissions before Spark jobs.
If a manual job still fails, run:

```bash
docker exec --user root amazon_reviews_spark_master bash -c 'chmod -R a+rwX /workspace/output'
```

## Hive or HDFS Not Ready

```bash
make check
docker compose logs hive-server
docker compose logs namenode
```

## Training Is Slow

The full Office Products dataset has millions of reviews. For a faster demo,
run the pipeline with a smaller JSONL sample first.
