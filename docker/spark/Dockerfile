FROM apache/spark:3.5.3

COPY --from=ghcr.io/astral-sh/uv:0.9.0 /uv /uvx /usr/local/bin/

USER root

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN uv pip install --system --python /usr/bin/python3 -r /tmp/requirements-spark.txt \
    && rm -f /tmp/requirements-spark.txt

USER spark
