FROM apache/airflow:2.7.1-python3.9
# Set airflow home env variable

WORKDIR /opt/airflow

# Switch to root to install system dependencies
USER root

# Upgrade pip first
RUN python3 -m pip install --upgrade pip
# Install required dependencies in a single step
RUN apt-get update && apt-get install -y \
    postgresql-client \
    awscli

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN python3 -m pip install --no-cache-dir -r /requirements.txt