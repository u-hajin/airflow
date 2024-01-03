import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import timedelta

email_str = Variable.get('email_target')
email_list = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_email_on_failure',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule='0 1 * * *',
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True,
        'email': email_list
    }
) as dag:
    @task(task_id='python_fail')
    def python_task_func():
        raise AirflowException('에러 발생')


    python_task_func()
    
    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1',
    )
    
    bash_successs = BashOperator(
        task_id='bash_success',
        bash_command='exit 0',
    )

# flow 없으므로 병렬로 개별 실행
