from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="kedro_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    run_kedro = DockerOperator(
        task_id="run_kedro",
        image="open_sky_pipeline-kedro:latest",
        command=["uv", "run", "kedro", "run"],
        docker_url="unix://var/run/docker.sock",
        network_mode="pipeline-net",
        auto_remove=True,
    )