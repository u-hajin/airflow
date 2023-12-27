import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023,12,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    def insert_postgres(ip, port, dbname, user, passwd, **kwagrs):
        import psycopg2
        from contextlib import closing
        
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwagrs.get('ti').dag_id
                task_id = kwagrs.get('ti').task_id
                run_id = kwagrs.get('ti').run_id
                msg = 'insert 수행'
                sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()
    
    
    insert_postgres = PythonOperator(
        task_id='insert_postgres',
        python_callable=insert_postgres,
        op_args=['172.28.0.3', '5432', 'usuyn', 'usuyn', 'usuyn']
    )
    
    insert_postgres
