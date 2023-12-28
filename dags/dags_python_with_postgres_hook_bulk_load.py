import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
        dag_id='dags_python_with_postgres_hook_bulk_load',
        start_date=pendulum.datetime(2023, 12, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, table_name, file_name, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(table_name, file_name)
    
    
    insert_postgres = PythonOperator(
        task_id='insert_postgres',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom',
                   'table_name':'TbCorona19CountStatus_bulk1',
                   'file_name':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbCorona19CountStatus.csv'}
    )
    
    insert_postgres