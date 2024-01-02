import pendulum

from airflow import DAG
from airflow import Dataset
from airflow.operators.bash import BashOperator

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1") # "dags_dataset_producer_1" key로 큐에 publish

with DAG(
    dag_id='dags_dataset_producer_1',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_dags_dataset_producer_1], # BaseOperator에 정의되어 있음, dag 안에 있는 task가 큐에 publish 함(dag 자체가 아닌)
        bash_command='echo "producer_1 수행 완료"'
    )
