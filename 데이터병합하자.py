import pandas as pd
from datetime import datetime, timedelta

# 파일 경로
weather_file = 'weather기상데이터.csv'
air_file = 'air대기데이터.csv'
water_file = 'water수온데이터.csv'

# 데이터 불러오기
weather_df = pd.read_csv(weather_file, encoding='utf-8-sig')
air_df = pd.read_csv(air_file, encoding='utf-8-sig')
water_df = pd.read_csv(water_file, encoding='utf-8-sig')

# 각 데이터프레임의 기준 열을 datetime 형식으로 변환
weather_df['dateTime'] = pd.to_datetime(weather_df['tm'])
air_df['dateTime'] = pd.to_datetime(air_df['dateTime'])
water_df['dateTime'] = pd.to_datetime(water_df['record_time'])

# 불필요한 열 제거
weather_df = weather_df.drop(columns=['tm'])
water_df = water_df.drop(columns=['record_time'])

# 각 데이터프레임의 데이터 확인
print("Weather DataFrame with dateTime")
print(weather_df.head())

print("Air DataFrame with dateTime")
print(air_df.head())

print("Water DataFrame with dateTime")
print(water_df.head())

# 병합
merged_df = pd.merge(air_df, weather_df, on='dateTime', how='outer')
print("Merged DataFrame after first merge")
print(merged_df.head())

merged_df = pd.merge(merged_df, water_df, on='dateTime', how='outer')
print("Merged DataFrame after second merge")
print(merged_df.head())

# 'date'와 'time' 열 추가
merged_df['date'] = merged_df['dateTime'].dt.date
merged_df['time'] = merged_df['dateTime'].dt.hour

# 열 이름 변경
merged_df = merged_df.rename(columns={
    'ta': 'temp',
    'hm': 'humidity',
    'pa': 'pressure',
    'ss': 'sunshine_hours',
    'dc10Tca': 'cloud_coverage',
    'dc10LmcsCa': 'low_mid_cloud_coverage',
    'so2Value': 'so2',
    'coValue': 'co',
    'o3Value': 'o3',
    'no2Value': 'no2',
    'pm10Value': 'pm10',
    'pm25Value': 'pm25',
    'temp': 'sea_temp'
})

# 'plant_idx' 열 추가
merged_df['plant_idx'] = 1

# 열 순서 재정렬
cols = ['date', 'time', 'temp', 'humidity', 'pressure', 'sunshine_hours', 'cloud_coverage', 
        'low_mid_cloud_coverage', 'sea_temp', 'so2', 'co', 'o3', 'no2', 'pm10', 'pm25', 'plant_idx']
merged_df = merged_df[cols]

# 최근 7일치 데이터 필터링
start_date = datetime.now() - timedelta(days=7)
merged_df = merged_df[merged_df['date'] >= start_date.date()]

# 비어 있는 값을 0으로 대체
merged_df = merged_df.fillna(0)

# 현재 날짜 가져오기
current_date_str = datetime.now().strftime('%Y%m%d')

# 병합된 데이터 저장
output_file = f'db용/solar_env_info_{current_date_str}.csv'
merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"병합된 데이터가 {output_file} 파일로 저장되었습니다.")
