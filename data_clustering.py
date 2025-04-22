import pandas as pd
from sklearn.cluster import KMeans

# 加載處理後的數據
all_data = pd.read_csv('data/processed_data.csv')

# 按路徑分割數據
# 假設有一個欄位 'route' 來識別不同的路徑
# 這部分需要具體的數據結構來實現

# K-means集群分析
# 使用各路段的旅行時間作為特徵
features = ['A21', 'A32', 'A43']  # 假設這些是旅行時間的欄位

# 設定集群數量為3
kmeans = KMeans(n_clusters=3, random_state=278613)
all_data['cluster'] = kmeans.fit_predict(all_data[features])

# 保存集群結果
all_data.to_csv('data/clustered_data.csv', index=False)
print("Clustering completed and saved to data/clustered_data.csv") 