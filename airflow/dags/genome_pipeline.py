from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
import json

def check_accuracy(**kwargs):
    with open('/opt/airflow/output/metrics.json') as f:
        metrics = json.load(f)
    return metrics.get("accuracy", 0) < 0.90

default_args = {
    'owner': 'cato',
    'start_date': datetime(2025, 5, 28),
}

with DAG(
    dag_id="gc_model_pipeline_docker",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    run_feature_engineering = DockerOperator(
        task_id='run_feature_engineering',
        image='gc_predictor_spark-preprocess',  # <-- must match built image name
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        command="spark-submit run_preprocess.py",
        mount_tmp_dir=False,
        volumes=["/absolute/path/to/gc_predictor_lib:/app/gc_predictor_lib"],
        environment={
            'AWS_ACCESS_KEY_ID': 'yourkey',
            'AWS_SECRET_ACCESS_KEY': 'yoursecret',
            'MINIO_ENDPOINT': 'minio:9000'
        }
    )

    run_model = DockerOperator(
        task_id='run_model',
        image='gc_predictor_ds-model',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        command="python run_model.py",
        mount_tmp_dir=False,
        volumes=["/absolute/path/to/output:/opt/airflow/output"],
        environment={
            'AWS_ACCESS_KEY_ID': 'yourkey',
            'AWS_SECRET_ACCESS_KEY': 'yoursecret',
            'MINIO_ENDPOINT': 'minio:9000'
        }
    )

    check_model_accuracy = ShortCircuitOperator(
        task_id="check_model_accuracy",
        python_callable=check_accuracy
    )

    notify_team = EmailOperator(
        task_id="notify_team",
        to="team@example.com",
        subject="[ALERT] Model Accuracy Below 90%",
        html_content="""
        <p>Accuracy below threshold. Please retrain or check pipeline.</p>
        """,
    )

    run_feature_engineering >> run_model >> check_model_accuracy >> notify_team
