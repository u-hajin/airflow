import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import timedelta

email_str = Variable.get('email_target')
email_list = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    default_args={
        'sla': timedelta(seconds=70),
        'email': email_list
    }
) as dag:
    
    task_sleep_30s_sla_70s = BashOperator(
        task_id='task_sleep_30s_sla_70s',
        bash_command='sleep 30'
    )
    
    task_sleep_60s_sla_70s = BashOperator(
        task_id='task_sleep_60s_sla_70s',
        bash_command='sleep 60'
    )
    
    task_sleep_10s_sla_70s = BashOperator(
        task_id='task_sleep_10s_sla_70s',
        bash_command='sleep 10'
    )
    
    task_sleep_10s_sla_30s = BashOperator(
        task_id='task_sleep_10s_sla_30s',
        bash_command='sleep 60',
        sla=timedelta(seconds=30)
    )
    
    task_sleep_30s_sla_70s >> task_sleep_60s_sla_70s >> task_sleep_10s_sla_70s >> task_sleep_10s_sla_30s
