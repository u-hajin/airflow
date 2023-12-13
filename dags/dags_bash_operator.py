import datetime
import pendulum # datetime 타입을 쉽게 쓸 수 있도록 하는 라이브러리

from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG에 대한 정의, 모든 DAG마다 필요
with DAG(
    dag_id="dags_bash_operator", # 사이트에서 보이는 이름 값, 파이썬 파일명과 상관 없으나 일치하는 것이 좋음
    schedule="0 0 * * *", # cron 스케줄, DAG이 언제 도는지, 주기(분 시 일 월 요일)
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), # DAG이 언제부터 돌 것인지, timezone 설정 필요
    catchup=False, # start_data가 2023.1.1, 현재가 2023.03.01이면 3월 1일부터 돌릴 것인지, 1월 1일부터 누락된 구간을 전부 돌릴 것인지 결정, False(누락 구간 돌리지 않음), True(누락되었던 구간 차례대로가 아닌 한번에 돌림)
    # dagrun_timeout=datetime.timedelta(minutes=60), # DAG이 60분 이상 돌면 실패하도록 설정된 것
    # tags=["example", "example2"], # 사이트 이름 값 밑 태그, 필수 요소 아님
    # params={"example_key": "example_value"}, # task에 공통적으로 넘겨줄 파라미터
) as dag:
    bash_t1 = BashOperator( # bash_t1 : task 객체명
        task_id="bash_t1", # 사이트 graph의 task 이름 값, 객체명과 연관 없으나 동일하게 줌
        bash_command="echo whoami", # 어떤 쉘 스크립트를 수행할 것인지
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    # task 수행 순서 관계
    bash_t1 >> bash_t2
