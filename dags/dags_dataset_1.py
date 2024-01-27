import pendulum

from airflow import DAG
from airflow import Dataset
from airflow.operators.bash import BashOperator

dataset_producer_1 = Dataset("dataset_producer_1")

with DAG(
    dag_id='dags_dataset_1',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_producer_1],
        bash_command='echo "1번 수행 완료"'
    )
    
    bash_sleep_task = BashOperator(
        task_id='bash_sleep_task',
        bash_command='sleep 45 && echo "1번 sleep task 수행 완료"'
    )
