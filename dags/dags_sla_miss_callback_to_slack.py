import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from config.sla_miss_callback_to_slack import sla_miss_callback_to_slack
from datetime import timedelta

with DAG(
    dag_id='dags_sla_miss_callback_to_slack',
    start_date=pendulum.datetime(2023, 1, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    sla_miss_callback=sla_miss_callback_to_slack
) as dag:
    
    task_sleep100_sla120 = BashOperator(
        task_id='task_sleep100_sla120',
        bash_command='sleep 100',
        sla=timedelta(minutes=2)
    )
    
    task_sleep100_sla180 = BashOperator(
        task_id='task_sleep100_sla180',
        bash_command='sleep 100',
        sla=timedelta(minutes=3)
    )
    
    task_sleep60_sla245 = BashOperator(
        task_id='task_sleep60_sla230',
        bash_command='sleep 60',
        sla=timedelta(seconds=245)
    )
    
    task_sleep60_sla250 = BashOperator(
        task_id='task_sleep60_sla250',
        bash_command='sleep 60',
        sla=timedelta(seconds=250)
    )

    task_sleep100_sla120 >> task_sleep100_sla180 >> task_sleep60_sla245 >> task_sleep60_sla250
