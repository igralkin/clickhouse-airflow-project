FROM apache/airflow:2.3.0

USER airflow

RUN pip install --user pandas clickhouse-connect