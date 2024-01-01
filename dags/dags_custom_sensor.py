import pendulum

from airflow import DAG
from sensors.seoul_api_date_sensor import SeoulApiDateSensor

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2023, 1, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    iot_vdata_sensor = SeoulApiDateSensor(
        task_id='iot_vdate_sensor',
        dataset_name='IotVdata020',
        base_date_col='REGIST_DT',
        day_off=0,
        poke_interval=600,
        mode='reschedule'
    )
    
    tv_corona19_vaccine_stat_new_sensor = SeoulApiDateSensor(
        task_id='tv_corona19_vaccine_stat_new_sensor',
        dataset_name='tvCorona19VaccinestatNew',
        base_date_col='S_VC_DT',
        day_off=-365,
        poke_interval=600,
        mode='reschedule'
    )
