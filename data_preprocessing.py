import pandas as pd
import numpy as np

# 加載數據
all_data = pd.read_csv('data/combined_data.csv')

# 數據清洗
# 處理缺失值
all_data.fillna(method='ffill', inplace=True)

# 轉換數據類型
all_data['timestamp'] = pd.to_datetime(all_data['timestamp'])

# 移除異常值
all_data = all_data[all_data['travel_time'] > 0]

# 特徵工程
# 從時間戳創建新的時間特徵
all_data['year'] = all_data['timestamp'].dt.year
all_data['month'] = all_data['timestamp'].dt.month
all_data['day'] = all_data['timestamp'].dt.day
all_data['weekday'] = all_data['timestamp'].dt.weekday
all_data['hour'] = all_data['timestamp'].dt.hour

# 添加時段分類
all_data['time_period'] = pd.cut(all_data['hour'], bins=[0, 6, 12, 18, 24], labels=['凌晨', '上午', '下午', '晚間'], right=False)

# 添加平假日分類
all_data['is_weekend'] = all_data['weekday'].apply(lambda x: 1 if x >= 5 else 0)

# 添加尖峰時間分類
all_data['is_peak'] = all_data['hour'].apply(lambda x: 1 if (7 <= x <= 9) or (16 <= x <= 18) else 0)

# 車種分類
all_data['vehicle_type'] = all_data['vehicle_type'].map({31: '小客車', 32: '小貨車', 41: '大型車', 42: '大型車', 5: '大型車'})

# 計算旅行時間
# 假設有一個欄位 'middle' 包含詳細資料
# 提取各測站的通過時間並計算旅行時間
# 這部分需要具體的數據結構來實現

# 保存處理後的數據
all_data.to_csv('data/processed_data.csv', index=False)
print("Data preprocessing completed and saved to data/processed_data.csv") 