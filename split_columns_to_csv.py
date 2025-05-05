import os
import pandas as pd

# 設定資料路徑
input_file = 'D:/highway_processed/2024_complete.csv'
output_dir = 'D:/highway_output/split_columns'

# 建立輸出目錄
os.makedirs(output_dir, exist_ok=True)

# 設定批次大小
batch_size = 100000  # 可以根據需要調整

# 讀取原始文件的欄位名稱
original_columns = pd.read_csv(input_file, nrows=0).columns.tolist()
expected_columns = set(original_columns) - {'year'}

# 記錄已處理的欄位數量
processed_columns = set()

# 讀取和處理數據
for chunk in pd.read_csv(input_file, chunksize=batch_size):
    # 將每個欄位存成獨立的 CSV 檔案，排除 'year' 欄位
    for column in chunk.columns:
        if column == 'year':
            continue
        output_file = os.path.join(output_dir, f"{column}.csv")
        chunk[[column]].to_csv(output_file, mode='a', index=False, header=not os.path.exists(output_file))
        processed_columns.add(column)

# 檢查處理結果
print(f"Processed columns: {processed_columns}")
print(f"Number of processed columns: {len(processed_columns)}")

# 自動檢查
if processed_columns == expected_columns:
    print("All columns processed successfully.")
else:
    print("Warning: Some columns may not have been processed correctly.")
    print(f"Expected columns: {expected_columns}")
    print(f"Missing columns: {expected_columns - processed_columns}")  