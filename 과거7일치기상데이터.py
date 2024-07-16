import requests
import pandas as pd
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
params = {
    'serviceKey': 'FZK05m20zmTL12eGY3mEIKaqzwuEi+p7Wq+ztuE/xfUOtDPejRF3Icqy3sfY0bhTCvB65turyS+6r2Q3LxSWtA==',
    'numOfRows': '999',
    'dataType': 'XML',
    'dataCd': 'ASOS',
    'dateCd': 'HR',
    'stnIds': '169'
}

# 시작 날짜와 종료 날짜 설정 (과거 7일치 데이터)
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')  # 7일 전 날짜로 설정
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # 어제 날짜로 설정

def fetch_page(start_date, end_date, page_no):
    params['startDt'] = start_date
    params['startHh'] = '01'
    params['endDt'] = end_date
    params['endHh'] = '23'
    params['pageNo'] = str(page_no)
    response = requests.get(url, params=params)
    return response.content

def fetch_data_for_period(start_date, end_date):
    period_params = {
        **params,
        'startDt': start_date,
        'startHh': '00',
        'endDt': end_date,
        'endHh': '23',
        'pageNo': '1'
    }
    response = requests.get(url, params=period_params)
    
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data for period {start_date} to {end_date}. Status code: {response.status_code}")
    
    root = ET.fromstring(response.content)
    result_code = root.find('.//resultCode')
    if result_code is not None and result_code.text != '00':
        result_msg = root.find('.//resultMsg').text
        raise ValueError(f"Error fetching data for period {start_date} to {end_date}: {result_msg}")
    
    total_count_elem = root.find('.//totalCount')
    if total_count_elem is not None and total_count_elem.text.isdigit():
        total_count = int(total_count_elem.text)
    else:
        print(f"Response content: {response.content}")
        raise ValueError(f"Invalid or missing totalCount in the response for period {start_date} to {end_date}")
    
    num_of_rows = int(params['numOfRows'])
    total_pages = (total_count // num_of_rows) + (1 if total_count % num_of_rows > 0 else 0)

    period_data = []

    with ThreadPoolExecutor(max_workers=10) as executor:  # 동시에 10개의 스레드 실행
        future_to_page = {executor.submit(fetch_page, start_date, end_date, page): page for page in range(1, total_pages + 1)}
        
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                data = future.result()
                root = ET.fromstring(data)
                for item in root.findall('.//item'):
                    record = {}
                    for child in item:
                        if child.text is not None:
                            record[child.tag] = child.text
                        else:
                            record[child.tag] = None
                    period_data.append(record)
            except Exception as exc:
                print(f'Page {page} generated an exception: {exc}')
    
    return period_data

print(f"Fetching data from {start_date} to {end_date}")

try:
    period_data = fetch_data_for_period(start_date, end_date)
    if period_data:
        # 데이터 프레임으로 변환
        df = pd.DataFrame(period_data)
        
        # 'tm' 열을 datetime 형식으로 변환
        df['tm'] = pd.to_datetime(df['tm'], format='%Y-%m-%d %H:%M')

        # 00시 데이터 제거
        df = df[df['tm'].dt.hour != 0]
        
        # 특정 속성만 남기기
        columns_to_keep = ['tm','ta','hm','pa','ss','dc10Tca','dc10LmcsCa']
        filtered_df = df[columns_to_keep]
        
        # 데이터 프레임을 CSV 파일로 저장
        file_name = "weather기상데이터.csv"
        filtered_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        
        print(f"Data saved to 'weather기상데이터.csv'")
    else:
        print(f"No data fetched for period {start_date} to {end_date}")
except Exception as e:
    print(f"Failed to fetch data for period {start_date} to {end_date}: {e}")
