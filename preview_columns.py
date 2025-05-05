import os
import pandas as pd

# 設定資料夾路徑
input_dir = 'D:/highway_output/split_columns'

# 獲取資料夾中的所有 CSV 文件
files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

# 預覽每個文件的前5行
for file in files:
    file_path = os.path.join(input_dir, file)
    try:
        data = pd.read_csv(file_path, nrows=5)  # 只讀取前5行
        print(f"\n文件: {file}")
        print(data.head())
    except Exception as e:
        print(f"讀取文件 {file} 時發生錯誤：{e}") 