import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook

with DAG(
        dag_id='dags_python_with_custom_hook_bulk_load',
        start_date=pendulum.datetime(2023, 12, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, table_name, file_name, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=table_name, file_name=file_name, delimeter=',', is_header=True, is_replace=True)
    
    
    insert_postgres = PythonOperator(
        task_id='insert_postgres',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom',
                   'table_name':'TbCorona19CountStatus_bulk2',
                   'file_name':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbCorona19CountStatus.csv'}
    )
    
    insert_postgres
