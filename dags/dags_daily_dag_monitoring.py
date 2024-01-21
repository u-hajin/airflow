import config.slack_block_builder as sb
import pandas as pd
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import closing

with DAG(
    dag_id='dags_daily_dag_monitoring',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule='0 8 * * *',
    catchup=False
) as dag:
    
    @task(task_id='get_daily_monitoring_result_task')
    def get_daily_monitoring_result_task():
        postgres_hook = PostgresHook(postgres_conn_id='conn-db-postgres-airflow')
        
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                with open('/opt/airflow/files/sqls/daily_dag_monitoring.sql', 'r') as sql_files:
                    cursor.execute("SET TIME ZONE 'Asia/Seoul';")
                    sql = '\n'.join(sql_files.readlines())  # ist로 가져오고(read_lines) 요소 사이에 \n 끼우고 다시 text 변환(join)
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    result = pd.DataFrame(result)
                    result.columns = ['dag_id', 'run_count', 'success_count', 'failed_count', 'running_count', 'last_failed_date', 'last_success_date', 'next_dagrun_data_interval_start', 'next_dagrun_interval_end']
                    return_blocks = []
                    
                    # 1) 실패 대상
                    failed_df = result.query("(failed_count > 0)")
                    return_blocks.append(sb.section_text("*2. 실패 대상*"))
                    
                    if not failed_df.empty:
                        for idx, row in failed_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*최근 실패 일자:* {row['last_failed_date']}\n*마지막 성공 일자:* {'없음' if str(row['last_success_date']) == 'NaT' else row['last_success_date']}"))
                    else:
                        return_blocks.append(sb.section_text("없음"))
                    
                    return_blocks.append(sb.divider())
                    
                    # 2) 미수행 대상
                    skipped_df = result.query("(run_count == 0)")
                    return_blocks.append(sb.section_text("*3. 미수행 대상*"))
                    
                    if not skipped_df.empty:
                        for idx, row in skipped_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*예정 일자:* {row['next_dagrun_data_interval_end']}"))
                    else:
                        return_blocks.append(sb.section_text("없음"))
                    
                    return_blocks.append(sb.divider())
                    
                    # 3) 수행 중 대상
                    running_df = result.query("(running_count > 0)")
                    return_blocks.append(sb.section_text("*4. 수행 중*"))
                    
                    if not running_df.empty:
                        for idx, row in running_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*배치 일자:* {row['next_dagrun_data_interval_start']}"))
                    else:
                        return_blocks.append(sb.section_text("없음"))
                        
                    return_blocks.append(sb.divider())
                    
                    # 4) 성공 대상
                    done_success_count = result.query("(failed_count == 0) and (run_count > 0) and (running_count == 0)").shape[0]  # 어제, 오늘 한번도 fail하지 않고, 한번 이상 돌았으며 현재 돌고 있지 않은 dag
                    yesterday = pendulum.yesterday('Asia/Seoul').strftime('%Y-%m-%d')
                    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
                    
                    return_blocks = [ sb.section_text(f"DAG 수행 현황 알림({yesterday} ~ {now})"),
                                      sb.divider(),
                                      sb.section_text(f"*1. 수행 대상 DAG 개수*: {result.shape[0]}\n    (1) 성공 DAG 개수: {done_success_count}\n    (2) 실패: {failed_df.shape[0]}\n    (3) 미수행: {skipped_df.shape[0]}\n    (4) 수행 중: {running_df.shape[0]}"),
                                      sb.divider()
                                    ] + return_blocks
                    
                    return return_blocks
    
    
    send_to_slack = SlackWebhookOperator(
        task_id='send_to_slack',
        slack_webhook_conn_id='conn_slack_airflow_bot',
        blocks='{{ ti.xcom_pull(task_ids="get_daily_monitoring_result_task") }}'
    )
    
    get_daily_monitoring_result_task() >> send_to_slack
