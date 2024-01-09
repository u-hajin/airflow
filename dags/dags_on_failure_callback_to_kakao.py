import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao
from datetime import timedelta

with DAG(
    dag_id='dags_on_failure_callback_to_kakao',
    start_date=pendulum.datetime(2023, 1, 1, tz='Asia/Seoul'),
    schedule='*/20 * * * *',
    catchup=False,
    default_args={
        'on_failure_callback': on_failure_callback_to_kakao,
        'execution_timeout': timedelta(seconds=60)
    }
) as dag:
    task_sleep_90 = BashOperator(
        task_id='task_sleep_90',
        bash_command='sleep 90'
    )
    
    task_exit_1 = BashOperator(
        trigger_rule='all_done',
        task_id='task_exit_1',
        bash_command='exit 1'
    )
    
    task_sleep_90 >> task_exit_1
