# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from __future__ import annotations

from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None
    BashOperator = None


if DAG is not None:
    with DAG(
        dag_id="amazon_reviews_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["amazon-reviews", "spark", "hive"],
    ) as dag:
        validate_raw_file = BashOperator(
            task_id="validate_raw_file",
            bash_command="PYTHONPATH=src python3 -m amazon_reviews_pipeline.ingestion.validate_raw_file --input data/raw/Office_Products.jsonl",
        )
        upload_to_hdfs = BashOperator(task_id="upload_to_hdfs", bash_command="bash scripts/upload_to_hdfs.sh")
        create_hive_tables = BashOperator(task_id="create_hive_tables", bash_command="make tables")
        bronze_to_silver = BashOperator(task_id="bronze_to_silver", bash_command="make transform")
        data_quality_checks = BashOperator(task_id="data_quality_checks", bash_command="make quality")
        silver_to_gold = BashOperator(task_id="silver_to_gold", bash_command="bash scripts/run_processing.sh")
        run_hive_queries = BashOperator(task_id="run_hive_queries", bash_command="bash scripts/run_hive_queries.sh")
        train_model = BashOperator(task_id="train_model", bash_command="make train")
        evaluate_model = BashOperator(task_id="evaluate_model", bash_command="PYTHONPATH=src python3 -m amazon_reviews_pipeline.ml.evaluate_model")

        validate_raw_file >> upload_to_hdfs >> create_hive_tables >> bronze_to_silver
        bronze_to_silver >> data_quality_checks >> silver_to_gold >> run_hive_queries >> train_model >> evaluate_model
