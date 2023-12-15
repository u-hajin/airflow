import pendulum

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=True, # 12월 1일부터 현재 날짜까지 구간 모두 수행
) as dag:

    @task(task_id="python_task")
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    
    show_templates()
