import pendulum

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id='tvCorona19VaccinestatNew_sensor',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='tvCorona19VaccinestatNew/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv',
        recursive=False,
        poke_interval=30,
        timeout=60 * 60 * 24, # 24시간
        mode='reschedule'
    )
    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo file sensor success'
    )

    tvCorona19VaccinestatNew_sensor >> bash_task
