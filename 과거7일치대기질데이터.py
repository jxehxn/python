import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# API URL과 파라미터 설정
url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
params = {
    'serviceKey': 'FZK05m20zmTL12eGY3mEIKaqzwuEi+p7Wq+ztuE/xfUOtDPejRF3Icqy3sfY0bhTCvB65turyS+6r2Q3LxSWtA==',
    'returnType': 'xml',
    'numOfRows': '10000',
    'pageNo': '1',
    'stationName': '홍도',
    'dataTerm': 'MONTH',
    'ver': '1.0'
}

data_list = []

# 데이터 수집
response = requests.get(url, params=params)
if response.status_code == 200:
    # XML 내용을 파싱
    root = ET.fromstring(response.content)
    
    # 결과 출력 및 데이터 수집
    for item in root.findall('.//item'):
        data = {
            'dateTime': item.find('dataTime').text,
            'so2Value': item.find('so2Value').text,
            'coValue': item.find('coValue').text,
            'o3Value': item.find('o3Value').text,
            'no2Value': item.find('no2Value').text,
            'pm10Value': item.find('pm10Value').text,
            'pm25Value': item.find('pm25Value').text
        }
        data_list.append(data)
else:
    print(f"API 요청 실패. 상태 코드: {response.status_code}")
    print(response.content.decode('utf-8'))

# 데이터프레임 생성
df = pd.DataFrame(data_list)

# "-" 값을 0으로 대체
df.replace('-', '0', inplace=True)

# "24:00"을 "00:00" 다음날로 변경
df['dateTime'] = df['dateTime'].str.replace('24:00', '00:00')
df['dateTime'] = pd.to_datetime(df['dateTime'], format='%Y-%m-%d %H:%M', errors='coerce')

# 날짜와 시간을 전처리
df['dateTime'] = df['dateTime'].fillna(df['dateTime'] + pd.Timedelta(days=1))

# 00시 데이터 제거
df = df[df['dateTime'].dt.hour != 0]

# dateTime 열을 기준으로 오름차순 정렬
df = df.sort_values(by='dateTime', ascending=True)

# 데이터프레임을 CSV 파일로 저장
df.to_csv('air대기데이터.csv', index=False)

print("데이터가 air대기데이터.csv 파일로 저장되었습니다.")
