import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    bash_var_2 = BashOperator(
        task_id='bash_var_2',
        bash_command='echo variable: {{ var.value.api_key }}'
    )
