import pendulum

from airflow import DAG
from airflow import Dataset
from airflow.operators.bash import BashOperator

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
    dag_id='dags_dataset_consumer_1',
    schedule=[dataset_dags_dataset_producer_1], # "dags_dataset_producer_1" key 값을 구독
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1 이 완료되면 수행"'
    )
