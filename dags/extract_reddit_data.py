from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from multiprocessing import Pool

import logging
logging.basicConfig(filename='dag_extract.log',level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

import subprocess

def extract_data(keyword):
    try:
        result = subprocess.run(["python", "/opt/airflow/scripts/reddit_extractor_data.py", "--subreddit", keyword],
                       capture_output=True, text=True, check=True, timeout=3600)
        logger.info("Data extracted successfully: %s", result.stdout)
    except subprocess.TimeoutExpired as e:
        logger.error("Data extraction took too long: %s", str(e))
        raise
    except subprocess.CalledProcessError as e:
        logger.error("Error: %s, Output: %s, Stderr: %s", str(e), e.output, e.stderr)
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        raise


default_args = {'start_date':datetime(2025,1,1)}

with DAG("extract_data", default_args=default_args) as dag:

    keywords = ["ProductReviews", "amazonreviews"]
    extract_tasks = []

    for keyword in keywords:
        task = PythonOperator(
            task_id = f"extract_reddit_data_{keyword}",
            python_callable = extract_data,
            op_args = [keyword]
        )
        extract_tasks.append(task)

    trigger_transform = TriggerDagRunOperator(
        task_id = "trigger_transform_data",
        trigger_dag_id = "transform_data",
        conf = {"parent_task_id": "extract_data"},
        dag = dag
    )


