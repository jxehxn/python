import pandas as pd
import mysql.connector

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
    exit()

# CSV 파일 읽기
csv_file_path = 'db용/prediction.info.csv'  # 실제 CSV 파일 경로로 대체
df = pd.read_csv(csv_file_path)

# plant_idx 속성이 이미 존재하는지 확인하고 없으면 추가
if 'plant_idx' not in df.columns:
    df.insert(0, 'plant_idx', 1)

# CSV 파일의 컬럼 이름을 데이터베이스 테이블의 컬럼 이름과 맞추기
df.columns = ['plant_idx', 'pred_date', 'pred_time', 'pred_power']

# pred_result 열을 추가하고 기본값 설정
if 'pred_result' not in df.columns:
    df['pred_result'] = None

# 데이터 확인
print("CSV Data:")
print(df.head())

# DB 테이블의 컬럼명 가져오기
cursor = conn.cursor()
cursor.execute("SHOW COLUMNS FROM prediction_info")
columns = cursor.fetchall()
db_columns = [column[0] for column in columns if column[0] != 'pred_idx']

# 데이터베이스 컬럼 확인
print("Database Columns:")
print(db_columns)

# CSV 파일의 컬럼과 DB 테이블의 컬럼 매칭
df = df[db_columns]

# 데이터 삽입
for i, row in df.iterrows():
    # 중복 여부 확인
    pred_date = row['pred_date']
    pred_time = row['pred_time']
    check_sql = "SELECT COUNT(*) FROM prediction_info WHERE pred_date = %s AND pred_time = %s"
    cursor.execute(check_sql, (pred_date, pred_time))
    count = cursor.fetchone()[0]
    
    if count == 0:  # 중복이 없는 경우에만 삽입
        sql = f"INSERT INTO prediction_info ({', '.join(db_columns)}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, tuple(row))
    else:
        print(f"Duplicate entry found for pred_date: {pred_date} and pred_time: {pred_time}")

# 커밋 및 연결 종료
conn.commit()
cursor.close()
conn.close()

print("Data uploaded successfully")
