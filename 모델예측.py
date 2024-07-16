import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.models import load_model
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 사용자 정의 메트릭: 10% 이내의 정확도 (모델 로드 시 필요)
def custom_accuracy(y_true, y_pred):
    return tf.reduce_mean(tf.cast(tf.abs((y_true - y_pred) / y_true) < 0.1, tf.float32))

# 현재 날짜 가져오기
current_date_str = datetime.now().strftime('%Y%m%d')

# 저장된 모델 불러오기
model_path = 'solar_power_prediction_model.h5'
model = load_model(model_path, custom_objects={'custom_accuracy': custom_accuracy,'mse': tf.keras.losses.MeanSquaredError()})

# 새로운 기상 데이터 불러오기
file_path = f'db용/solar_env_info_{current_date_str}.csv'
data = pd.read_csv(file_path)

# 결측치 처리 및 '-' 값을 0으로 대체
data.replace('-', 0, inplace=True)
data = data.dropna()

# 데이터가 제대로 불러와졌는지 확인
print("Initial Data Check:")
print(data.head())

# 특징 선택 (영어로 변경)
features = data[['temp', 'humidity', 'pressure', 'sunshine_hours', 'cloud_coverage', 'low_mid_cloud_coverage', 'sea_temp', 'so2', 'co', 'o3', 'no2', 'pm10', 'pm25']]
print("Selected Features Check:")
print(features.head())

# 데이터 스케일링
scaler_features = MinMaxScaler()
features_scaled = scaler_features.fit_transform(features)
print("Scaled Features Check:")
print(features_scaled[:5])

# 시퀀스 생성 함수
def create_sequences(data, seq_length):
    xs = []
    for i in range(len(data) - seq_length + 1):
        x = data[i:i + seq_length]
        xs.append(x)
    return np.array(xs)

seq_length = 24
X = create_sequences(features_scaled, seq_length)
print("Created Sequences Check:")
print(X[:1])  # 첫 번째 시퀀스만 출력

# 모델 예측
predictions = model.predict(X)
print("Predictions Check:")
print(predictions[:5])

# 마지막 시퀀스를 기반으로 미래 48시간 예측
last_sequence = features_scaled[-seq_length:]  # 마지막 시퀀스를 가져옴
future_predictions = []

for i in range(48):
    pred = model.predict(last_sequence[np.newaxis, :, :])
    future_predictions.append(pred[0, 0])
    new_sequence = np.roll(last_sequence, -1, axis=0)  # 시퀀스를 한 칸 앞으로 이동
    new_sequence[-1] = pred  # 새로운 예측값 추가
    last_sequence = new_sequence

# 예측된 발전량을 스케일 복원
target_file_path = '기본_태양광데이터_운량_5월부터11월_발전량만.csv'
target_data = pd.read_csv(target_file_path)
scaler_target = MinMaxScaler()
scaler_target.fit(target_data[['발전량(kW)']])
future_predictions = scaler_target.inverse_transform(np.array(future_predictions).reshape(-1, 1)).flatten()
future_predictions = np.maximum(future_predictions, 0)  # 음수 값을 0으로 변환
print("Future Predictions (Rescaled) Check:")
print(future_predictions)

# 결과를 데이터프레임으로 변환
last_date = pd.to_datetime(data['date'].iloc[-1])
last_hour = int(data['time'].iloc[-1])
future_dates = pd.date_range(start=last_date + pd.Timedelta(hours=last_hour + 1), periods=48, freq='h')
result_df = pd.DataFrame({'pred_date': future_dates.date, 'pred_time': future_dates.hour, 'pred_power': future_predictions})
print("Result DataFrame Check:")
print(result_df.head())

# plant_idx 속성 추가
result_df.insert(0, 'plant_idx', 1)

# pred_time이 0인 행 제거
result_df = result_df[result_df['pred_time'] != 0]

# 결과를 CSV 파일로 저장 (한글 인코딩 문제 해결)
result_file_path = 'db용/prediction.info.csv'
result_df.to_csv(result_file_path, index=False, encoding='utf-8-sig')

print(f"결과가 저장되었습니다: {result_file_path}")

# 예측 결과 시각화
plt.figure(figsize=(15, 7))
plt.plot(result_df['pred_date'].astype(str) + ' ' + result_df['pred_time'].astype(str), result_df['pred_power'], label='Predicted Power Generation (kW)', color='blue')
plt.xlabel('Date Time')
plt.ylabel('Power Generation (kW)')
plt.title('Predicted Power Generation for Next 48 Hours')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.show()
