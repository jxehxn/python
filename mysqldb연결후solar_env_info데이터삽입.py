import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

# MySQL 데이터베이스에 연결
try:
    conn = mysql.connector.connect(
        host='project-db-cgi.smhrd.com',
        user='mp_21K_bigdata22_p3_3',
        password='smhrd3',
        database='mp_21K_bigdata22_p3_3',
        port=3307  # 포트 번호
    )
    if conn.is_connected():
        print("Successfully connected to the database")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    conn.close()
    exit()


# 현재 날짜 가져오기
current_date_str = datetime.now().strftime('%Y%m%d')

# CSV 파일 읽기
csv_file_path = f'db용/solar_env_info_{current_date_str}.csv'  # 실제 CSV 파일 경로로 대체
df = pd.read_csv(csv_file_path)

# DB 테이블의 컬럼명 가져오기
cursor = conn.cursor()
cursor.execute(f"SHOW COLUMNS FROM solar_env_info")
columns = cursor.fetchall()
db_columns = [column[0] for column in columns if column[0] != 'env_idx']

# CSV 파일의 컬럼과 DB 테이블의 컬럼 매칭
df = df[db_columns]

# 데이터 삽입
for i, row in df.iterrows():
    # 중복 여부 확인
    date = row['date']
    time = row['time']
    check_sql = f"SELECT COUNT(*) FROM solar_env_info WHERE date = %s AND time = %s"
    cursor.execute(check_sql, (date, time))
    count = cursor.fetchone()[0]
    
    if count == 0:  # 중복이 없는 경우에만 삽입
        sql = f"INSERT INTO solar_env_info ({', '.join(db_columns)}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, tuple(row))
    else:
        print(f"Duplicate entry found for date: {date} and time: {time}")

# 커밋 및 연결 종료
conn.commit()
cursor.close()
conn.close()

print("Data uploaded successfully")
