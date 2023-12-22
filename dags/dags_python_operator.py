import datetime
import pendulum
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *", # daily
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    def select_fruits():
        fruit = ["APPLE", "BANANA", "ORANGE", "AVOCADO"]
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id="py_t1",
        python_callable=select_fruits # 어떤 함수를 실행시키는지
    )
    
    # @task(task_id='py_t1')
    # def select_fruits():
    #     fruit = ["APPLE", "BANANA", "ORANGE", "AVOCADO"]
    #     rand_int = random.randint(0, 3)
    #     print(fruit[rand_int])
    #     raise AirflowException('exception 발생')
        

    py_t1
