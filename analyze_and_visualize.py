import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import gc
from datetime import datetime

# Initialize variables for aggregation
output_file = 'D:/highway_processed/aggregated_data.csv'

# Load data in chunks
chunk_size = 100000  # Adjusted chunk size for better memory management
chunk_count = 0

# Estimate total number of rows for percentage calculation
try:
    with open('D:/highway_processed/2024_complete.csv', 'r', encoding='utf-8') as f:
        total_rows = sum(1 for line in f) - 1  # Subtract 1 for header
except FileNotFoundError:
    print("Error: Input file not found.")
    total_rows = 0
except UnicodeDecodeError:
    print("Error: Unable to decode file. Please check the file encoding.")
    total_rows = 0

start_time = datetime.now()
processed_rows = 0

if total_rows > 0:
    for chunk in pd.read_csv('D:/highway_processed/2024_complete.csv', chunksize=chunk_size):
        chunk_start = datetime.now()
        print(f"Processing chunk {chunk_count + 1}...")
        # Convert 'time_end' and 'time' to datetime with specified format
        chunk['time_end'] = pd.to_datetime(chunk['time_end'], format='%Y-%m-%d %H:%M:%S')
        chunk['time'] = pd.to_datetime(chunk['time'], format='%Y-%m-%d %H:%M:%S')

        # Calculate travel time
        time_diff = chunk['time_end'] - chunk['time']
        chunk['travel_time'] = time_diff.dt.total_seconds() / 60  # Convert to minutes

        # Save processed chunk to CSV
        if not pd.io.common.file_exists(output_file):
            chunk.to_csv(output_file, index=False)
        else:
            chunk.to_csv(output_file, mode='a', header=False, index=False)

        # Release resources
        del chunk
        gc.collect()

        # Update and print progress
        chunk_count += 1
        processed_rows += chunk_size
        progress_percentage = (processed_rows / total_rows) * 100
        chunk_time = (datetime.now() - chunk_start).total_seconds()
        total_time = (datetime.now() - start_time).total_seconds()

        # Estimate remaining time
        if processed_rows > 0 and total_rows > processed_rows:
            avg_time_per_row = total_time / processed_rows
            est_remaining_time = avg_time_per_row * (total_rows - processed_rows)
            est_remaining_min = est_remaining_time / 60
            print(f"已處理：{processed_rows:,} 筆 (估計進度: {progress_percentage:.1f}%)")
            print(f"本批處理時間：{chunk_time:.1f} 秒 (每秒約 {chunk_size/chunk_time:.0f} 筆)")
            print(f"累計處理時間：{total_time:.1f} 秒 (估計剩餘: {est_remaining_min:.1f} 分鐘)")
        else:
            print(f"已處理：{processed_rows:,} 筆")
            print(f"本批處理時間：{chunk_time:.1f} 秒 (每秒約 {chunk_size/chunk_time:.0f} 筆)")
            print(f"累計處理時間：{total_time:.1f} 秒")
else:
    print("No data to process.")

# Load the aggregated data for visualization
if total_rows > 0:
    aggregated_data = pd.read_csv(output_file)

    # Calculate cluster statistics
    cluster_stats = aggregated_data.groupby('cluster').agg({'travel_time': ['mean', 'std'], 'speed': 'mean'})
    print(cluster_stats)

    # Plot cluster travel speed comparison
    plt.figure(figsize=(10, 6))
    sns.barplot(data=aggregated_data, x='route', y='speed', hue='cluster')
    plt.title('Cluster Travel Speed Comparison')
    plt.xlabel('Route')
    plt.ylabel('Average Speed (km/h)')
    plt.legend(title='Cluster')
    plt.show()

    # Plot cluster time distribution
    plt.figure(figsize=(10, 6))
    sns.heatmap(aggregated_data.pivot_table(index='weekday', columns='time_period', values='travel_time', aggfunc='mean'), cmap='coolwarm')
    plt.title('Cluster Time Distribution')
    plt.xlabel('Time Period')
    plt.ylabel('Weekday')
    plt.show()

    # Plot traffic volume time series
    plt.figure(figsize=(10, 6))
    aggregated_data.groupby('month')['vehicle_count'].sum().plot()
    plt.title('Traffic Volume Time Series')
    plt.xlabel('Month')
    plt.ylabel('Vehicle Count')
    plt.show() 