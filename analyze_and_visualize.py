import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 加載集群結果數據
clustered_data = pd.read_csv('data/clustered_data.csv')

# 計算集群統計量
cluster_stats = clustered_data.groupby('cluster').agg({'travel_time': ['mean', 'std'], 'speed': 'mean'})
print(cluster_stats)

# 繪製集群旅行時速對比圖
plt.figure(figsize=(10, 6))
sns.barplot(data=clustered_data, x='route', y='speed', hue='cluster')
plt.title('Cluster Travel Speed Comparison')
plt.xlabel('Route')
plt.ylabel('Average Speed (km/h)')
plt.legend(title='Cluster')
plt.show()

# 繪製集群時間分布圖
plt.figure(figsize=(10, 6))
sns.heatmap(clustered_data.pivot_table(index='weekday', columns='time_period', values='travel_time', aggfunc='mean'), cmap='coolwarm')
plt.title('Cluster Time Distribution')
plt.xlabel('Time Period')
plt.ylabel('Weekday')
plt.show()

# 繪製車流量時間序列圖
plt.figure(figsize=(10, 6))
clustered_data.groupby('month')['vehicle_count'].sum().plot()
plt.title('Traffic Volume Time Series')
plt.xlabel('Month')
plt.ylabel('Vehicle Count')
plt.show() 