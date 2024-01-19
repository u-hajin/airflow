import pendulum

from airflow import DAG
from airflow import Dataset
from airflow.operators.bash import BashOperator

dataset_producer_1 = Dataset("dataset_producer_1")
dataset_producer_2 = Dataset("dataset_producer_2")


with DAG(
    dag_id='dags_dataset_2',
    schedule=[dataset_producer_1],
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_producer_2],
        bash_command='echo {{ ti.run_id }} && echo "1번이 완료되면 수행" && echo "2번 수행 완료"'
    )