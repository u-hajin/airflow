import pendulum
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1", # 월 단위 작업, 매월 첫번째 토요일 0시 10분
    start_date=pendulum.datetime(2023, 12, 14, tz="Asia/Seoul"),
    catchup=False
) as dag:
  
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORAGNE", # worker 컨테이너가 sh 위치를 알 수 있도록, yaml 파일에서 /opt/airflow/plugins까지 인식되도록 설정했음, 그 이하 파일 인식 가능
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    t1_orange >> t2_avocado
