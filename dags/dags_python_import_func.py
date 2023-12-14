import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp # plugins 폴더 이하 경로부터 작성해야 airflow에서 오류 발생 없음

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    task_get_sftp = PythonOperator(
        task_id="task_get_sftp",
        python_callable=get_sftp
    )
