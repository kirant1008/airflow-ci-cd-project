FROM apache/airflow:2.7.1-python3.9
USER root
RUN pip install --no-cache-dir boto3 psycpg2-binary requests flake8 pytest
USER airflow
COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts