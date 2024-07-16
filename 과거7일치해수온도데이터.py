import requests
import pandas as pd
from datetime import datetime, timedelta

# API URL과 서비스 키 설정
url = 'http://www.khoa.go.kr/api/oceangrid/tideObsTemp/search.do'
service_key = 'niqmnqK/zDRX55oaecW5Dg=='  # 실제 서비스 키로 변경 필요

# 날짜 범위 설정 (과거 7일)
end_date = datetime.strptime('20240609', '%Y%m%d')
start_date = end_date - timedelta(days=6)

# 데이터 저장을 위한 리스트 초기화
all_data = []

# 날짜 범위 내에서 데이터 수집
for single_date in (start_date + timedelta(n) for n in range(7)):
    date_str = single_date.strftime('%Y%m%d')
    params = {
        'DataType': 'tideObsTemp',
        'ServiceKey': service_key,
        'ObsCode': 'DT_0035',  # 관측소 코드
        'Date': date_str,      # 날짜
        'ResultType': 'json'   # 결과 형식
    }
    
    # 데이터 수집
    response = requests.get(url, params=params)
    
    # 응답 상태 코드 확인
    print(f'Response status code for {date_str}: {response.status_code}')
    
    # 응답 데이터 확인
    content = response.json()
    print(f'Content for {date_str}: {content}')
    
    # 응답 데이터 파싱 및 저장 (JSON 형식인 경우)
    if response.status_code == 200:
        try:
            data = content['result']
            
            # data 부분 추출
            records = data['data']
            
            # 각 레코드에 시간 및 해수온도 속성 추가
            for record in records:
                record_time_str = record['record_time']
                try:
                    record_time = datetime.strptime(record_time_str, '%Y-%m-%d %H:%M')
                except ValueError:
                    record_time = datetime.strptime(record_time_str, '%Y-%m-%d %H:%M:%S')
                
                record['record_time'] = record_time.strftime('%Y-%m-%d %H:%M:%S')
                record['time'] = record_time.hour
                record['date'] = record_time.strftime('%Y-%m-%d')
                record['sea_temp'] = record['water_temp']  # water_temp 속성을 sea_temp로 변경
                del record['water_temp']  # water_temp 속성 삭제
                
                # 필요한 속성만 남기기
                filtered_record = {
                    'record_time': record['record_time'],
                    'date': record['date'],
                    'time': record['time'],
                    'sea_temp': record['sea_temp']
                }
                
                # 데이터 리스트에 추가
                all_data.append(filtered_record)
            
        except ValueError as e:
            print(f'JSON Parse Error on {date_str}: {e}')
    else:
        print(f'Failed to retrieve data for {date_str}')

# DataFrame으로 변환
df = pd.DataFrame(all_data)

# DataFrame 내용 확인
print(df)

# 매 시간 첫 번째 기록만 남기기
if not df.empty:
    df = df.groupby(['date', 'time']).first().reset_index()

# 1시부터 23시까지 데이터만 남기기
df = df[(df['time'] >= 1) & (df['time'] <= 23)]

# DataFrame 내용 확인
print(df)

# CSV 파일로 저장
df.to_csv('water수온데이터.csv', index=False, encoding='utf-8-sig')
print('water수온데이터csv')
