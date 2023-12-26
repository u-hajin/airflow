import pandas as pd

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')
    
    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt
        
        
    def execute(self, context):
        import os
        
        connection = BaseHook.get_connection(self.http_conn_id) # connection 정보, 객체 얻기
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}' # start, end 행 제외 모두 작성된 url
        
        total_row_df = pd.DataFrame() # 빈 DataFrame
        start_row = 1
        end_row = 1000
        
        while True:
            self.log.info(f'시작: {start_row}')
            self.log.info(f'끝: {end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df]) # dataframe 합치기
            
            if len(row_df) < 1000:
                break # 더 읽을 data 없음
            else:
                start_row = end_row + 1
                end_row += 1000
                
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)
        
        
    def _call_api(self, base_url, start_row, end_row):
        import requests # HTTP GET 요청 라이브러리
        import json
        
        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }
        
        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers)
        contents = json.loads(response.text) # dictionary 반환
        
        key_nm = list(contents.keys())[0] # stationInfo key를 list로 만들고 0번째 요소 가져옴 -> stationInfo string
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)
        
        return row_df
