import pendulum

from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id='dags_time_sensor',
    start_date=pendulum.datetime(2023, 1, 6, 0, 0, 0),
    end_date=pendulum.datetime(2023, 1, 6, 1, 0, 0),    # 구간 1시간, 10분마다 돌기 때문에 7번
    catchup=True,
    schedule='*/10 * * * *',
) as dag:
    sync_sensor = DateTimeSensor(   # 목표로 하는 시간까지 기다리는 sensor
        task_id='sync_sensor',
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )
