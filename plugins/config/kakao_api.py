import json
import os
import pendulum
import requests

from airflow.models import Variable
from dateutil.relativedelta import relativedelta

REDIRECT_URL = 'https://example.com/oauth'

def _refresh_token_to_variable():
    client_id = Variable.get('kakao_client_secret')
    tokens = eval(Variable.get('kakao_tokens'))
    refresh_token = tokens.get('refresh_token')
    url = 'https://kauth.kakao.com/oauth/token'
    data = {
        'grant_type': 'refresh_token',
        'client_id': f'{client_id}',
        'refresh_token': f'{refresh_token}',
    }
    
    response = requests.post(url, data=data)
    result = response.json()
    new_access_token = result.get('access_token')
    new_refresh_token = result.get('refresh_token')     # refresh token 만료 기간 30일 미만이면 refresh_token 값이 포함되어 리턴됨
    
    if new_access_token:
        tokens['access_token'] = new_access_token
    if new_refresh_token:
        tokens['refresh_token'] = new_refresh_token
    
    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
    tokens['updated'] = now
    os.system(f'airflow variable set kakao_tokens "{tokens}"')
    print('variable 업데이트 완료(key: kakao_tokens)')
    
    
def send_kakao_message(talk_title: str, content: dict):
    '''
    content:{'title1':'content1', 'title2':'content2', ...}
    '''
    
    try_count = 0
    while True:
        ### get Access token
        tokens = eval(Variable.get('kakao_tokens'))
        access_token = tokens.get('access_token')
        content_list = []
        button_list = []
        
        for title, msg in content.items():
            content_list.append({
                'title': f'{title}',
                'description': f'{msg}',
                'image_url': '',
                'image_width': 40,
                'image_height': 40,
                'link': {
                    'web_url': '',
                    'mobile_web_url': ''
                }
            })
            
            button_list.append({
                'title': '',
                'link': {
                    'web_url': '',
                    'mobile_web_url': ''
                }
            })
            
        list_data = {
            'object_type': 'list',
            'header_title': f'{talk_title}',
            'header_link': {
                'web_url': '',
                'mobile_web_url': '',
                'android_execution_params': 'main',
                'ios_execution_params': 'main'
            },
            'contents': content_list,
            'buttons': button_list
        }
        
        send_url = 'https://kapi.kakao.com/v2/api/talk/memo/default/send'
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        
        data = {'template_object': json.dumps(list_data)}   # list_data를 json으로 변환
        response = requests.post(send_url, headers=headers, data=data)
        
        print(f'try 횟수: {try_count}, response 상태: {response.status_code}')
        try_count += 1
        
        if response.status_code == 200:     # 200: 정상
            return response.status_code
        elif response.status_code == 400:   # 400: Bad Request, 무조건 break 하도록 return
            return response.status_code
        elif response.status_code == 401 and try_count <= 2:    # 401: Unauthorized (토큰 만료 등)
            _refresh_token_to_variable()
        elif response.status_code != 200 and try_count >= 3:    # 400, 401 에러가 아닐 경우 3회 시도 때 종료
            return response.status_code
