import pendulum

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
    
    
    @task(task_id='python_xcom_push_by_return2')
    def xcom_push_result2(**kwargs):
        return 'Success 2'
    
    
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull()
        value2 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        
        print('key, task_ids 명시 X : ' + value1)
        print('task_ids 명시 : ' + value2)
        
    
    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)
        
    
    python_xcom_push_by_return = xcom_push_result() # 단순히 String 값이 아닌 airflow task 객체
    python_xcom_push_by_return2 = xcom_push_result2()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> python_xcom_push_by_return2 >> xcom_pull_1()
