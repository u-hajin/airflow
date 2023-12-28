import psycopg2
import pandas as pd

from airflow.hooks.base import BaseHook

class CustomPostgresHook(BaseHook):
    
    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id
    
    
    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port
        
        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port) # DB 연결 시도
        return self.postgres_conn # session 객체 리턴
    
    
    def bulk_load(self, table_name, file_name, delimeter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine
        
        self.log.info('적재 대상 파일: ' + file_name)
        self.log.info('테이블: ' + table_name)
        self.get_conn() # self.host, self.user,.. 변수들이 필요해 한번 실행
        
        header = 0 if is_header else None # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append' # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimeter)
        
        for col in file_df.columns: # 각 column이 가진 값들에 대해 replace 시도
            try:
                # string 문자열이 아닐 경우 continue, 숫자형 column에 대해서는 수행하지 않음
                file_df[col] = file_df[col].str.replace('\r\n', '') # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행 문자 제거')
            except:
                continue
        
        self.log.info('적재 건수: ' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                       )
