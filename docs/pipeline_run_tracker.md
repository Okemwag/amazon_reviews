# Pipeline Run Tracker

Date: 2026-05-09

## Execution Status

| Step | Status | Notes |
| --- | --- | --- |
| Fix Docker Compose paths | Done | Updated mounts and env files for the new `configs/`, `hive/`, `output/`, and `src/amazon_reviews_pipeline/` layout. |
| Validate Docker Compose config | Done | `docker compose config` completed successfully. |
| Start Docker services | Done | Containers were created; Hive Metastore issue fixed by mounting `hive/hive-site.xml` into Hive containers. |
| Check container status | Done | `docker compose ps` shows Hadoop/YARN and Hive services running. |
| Validate Python package | Done | `make validate` completed successfully. |
| Ingest raw data to HDFS | Done | Uploaded `Office_Products.jsonl` to HDFS and wrote manifest `output/metrics/manifests/Office_Products_20260509T183458Z.json`. |
| Create Hive tables | Done | Created `raw_reviews` and `cleaned_reviews`; DDL now quotes reserved `` `timestamp` `` and avoids full raw count. |
| Run Spark processing | Pending | Run `make transform`. |
| Run data quality checks | Pending | Run `make quality`. |
| Run Hive analysis queries | Pending | Run `make analysis`. |
| Train sentiment model | Pending | Run `make train`. |

## Open Issues

- None blocking right now.

## Fix Log

- Mounted `./hive/hive-site.xml` into `/opt/hive/conf/hive-site.xml` for both `hive-metastore` and `hive-server`.
- Recreated `hive-metastore` and `hive-server`.
- Verified HiveServer2 with `beeline -u jdbc:hive2://localhost:10000 -e 'SHOW DATABASES;'`.
- Added Spark hostnames and `SPARK_LOCAL_HOSTNAME` settings so Spark master and worker resolve each other.
- Verified Spark master started at `spark://spark-master:7077` and worker registered with 2 cores and 2 GiB RAM.
- Confirmed HDFS readiness command works with `hdfs dfs -ls /`.
- Updated ingestion to print when it scans the 5.4 GB input and to compute row count plus SHA-256 in one pass instead of two.
- Fixed Docker command wrapper from `bash -lc` to `bash -c`; login shell lost the Hadoop `hdfs` path inside the NameNode container.
- Fixed Hive DDL reserved word issue by quoting `` `timestamp` ``.
- Removed the trailing full `COUNT(*)` from table DDL because it launched a slow Hive-on-MapReduce validation query on the 5.4 GB raw file.
