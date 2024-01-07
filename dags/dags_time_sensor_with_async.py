import pendulum

from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id='dags_time_sensor_with_async',
    start_date=pendulum.datetime(2023, 1, 6, 0, 0, 0),
    end_date=pendulum.datetime(2023, 1, 6, 1, 0, 0),
    catchup=True,
    schedule='*/10 * * * *',
) as dag:
    sync_sensor = DateTimeSensor(
        task_id='sync_sensor',
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )
