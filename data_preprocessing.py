# 使用GPU加速的資料處理程式

import os
import traceback
import sys
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

def process_chunk_cpu(chunk):
    """使用CPU處理資料區塊"""
    # 重命名欄位，確保欄位名稱一致且有意義
    new_columns = ['id', 'time', 'location_start', 'time_end', 'location_end', 'value', 'flag', 'path_info']
    
    # 如果欄位數量不符，調整列名長度
    if len(chunk.columns) != len(new_columns):
        print(f"警告：欄位數量不符 (原有 {len(chunk.columns)}, 期望 {len(new_columns)})")
        if len(chunk.columns) > len(new_columns):
            new_columns.extend([f'extra_{i}' for i in range(len(chunk.columns) - len(new_columns))])
        else:
            new_columns = new_columns[:len(chunk.columns)]
    
    # 重命名欄位
    chunk.columns = new_columns
    
    # 轉換時間格式
    try:
        chunk['timestamp'] = pd.to_datetime(chunk['time'])
    except Exception as e:
        print(f"時間轉換錯誤: {e}")
        try:
            chunk['timestamp'] = pd.to_datetime(chunk['time'], errors='coerce')
            # 移除無法解析的時間
            invalid_count = chunk['timestamp'].isna().sum()
            if invalid_count > 0:
                print(f"時間轉換: 有 {invalid_count} 筆資料無法解析 (共 {len(chunk)} 筆)")
                chunk = chunk.dropna(subset=['timestamp'])
                print(f"已刪除無效時間資料，剩餘 {len(chunk)} 筆")
        except Exception as e2:
            print(f"時間轉換完全失敗: {e2}")
            raise
    
    # 創建時間特徵
    chunk['year'] = chunk['timestamp'].dt.year
    chunk['month'] = chunk['timestamp'].dt.month
    chunk['day'] = chunk['timestamp'].dt.day
    chunk['weekday'] = chunk['timestamp'].dt.weekday
    chunk['hour'] = chunk['timestamp'].dt.hour
    
    # 添加時段分類
    chunk['time_period'] = pd.cut(chunk['hour'], 
                                bins=[0, 6, 12, 18, 24], 
                                labels=['凌晨', '上午', '下午', '晚間'], 
                                right=False)
    
    # 添加平假日分類
    chunk['is_weekend'] = chunk['weekday'].apply(lambda x: 1 if x >= 5 else 0)
    
    # 添加尖峰時間分類
    chunk['is_peak'] = chunk['hour'].apply(lambda x: 1 if (7 <= x <= 9) or (16 <= x <= 18) else 0)
    
    # 嘗試轉換數值欄位
    try:
        chunk['value'] = pd.to_numeric(chunk['value'], errors='coerce')
        
        # 移除異常值 (負值或過大值)
        chunk = chunk[chunk['value'] > 0]
        chunk = chunk[chunk['value'] < 1000]  # 假設正常值上限
    except Exception as e:
        print(f"數值轉換或過濾錯誤: {e}")
    
    return chunk

def apply_clustering(data):
    # Select features for clustering
    features = data[['value', 'hour', 'is_peak']]
    
    # Apply K-means clustering
    kmeans = KMeans(n_clusters=3, random_state=42)
    data['cluster'] = kmeans.fit_predict(features)
    
    return data

def main():
    # 設定資料路徑
    input_dir = 'D:/highway_output'
    output_dir = 'D:/highway_processed'
    
    # 建立輸出目錄
    os.makedirs(output_dir, exist_ok=True)
    
    # 設定輸入和輸出檔案路徑
    input_file = os.path.join(input_dir, '2024_M06A.csv')
    output_file = os.path.join(output_dir, '2024_complete.csv')
    
    print(f"\n開始資料預處理... (CPU模式)")
    print(f"讀取檔案：{input_file}")
    
    # 設定分批大小
    chunk_size = 1000000
    
    # 檢查檔案是否存在
    if not os.path.exists(input_file):
        print(f"錯誤: 找不到輸入檔案 {input_file}")
        return
    
    # 使用檔案大小來估計資料量
    file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
    print(f"檔案大小：{file_size_mb:.1f} MB")
    
    # 讀取第一行以檢查欄位名稱
    try:
        header = pd.read_csv(input_file, nrows=0)
        
        print("\n資料欄位列表：")
        for i, col in enumerate(header.columns):
            print(f"{i+1}. {col}")
        
        # 估計行數
        est_bytes_per_row = 150
        est_total_rows = int(os.path.getsize(input_file) / est_bytes_per_row)
        print(f"估計資料筆數：約 {est_total_rows:,} 筆 (實際數量可能不同)")
        
    except Exception as e:
        print(f"讀取欄位時出錯：{str(e)}")
        est_total_rows = 1000000  # 預設估計值
    
    # 使用CPU處理資料
    process_chunk = process_chunk_cpu
    
    # 分批讀取和處理資料
    start_time = datetime.now()
    processed_rows = 0
    
    try:
        reader = pd.read_csv(input_file, chunksize=chunk_size)
        
        for chunk_num, chunk in enumerate(reader, 1):
            chunk_start = datetime.now()
            print(f"\n處理第 {chunk_num} 批 (大小: {len(chunk):,} 筆)...")
            
            if chunk_num == 1:
                print("\n資料前5筆原始預覽：")
                print(chunk.head())
            
            # 處理缺失值
            chunk = chunk.ffill()
            
            # 處理資料
            processed_chunk = process_chunk(chunk)
            
            # 應用聚類
            processed_chunk = apply_clustering(processed_chunk)
            
            # 儲存處理後的資料
            if chunk_num == 1:
                processed_chunk.to_csv(output_file, index=False)
            else:
                processed_chunk.to_csv(output_file, mode='a', header=False, index=False)
            
            # 更新進度
            processed_rows += len(processed_chunk)
            chunk_time = (datetime.now() - chunk_start).total_seconds()
            total_time = (datetime.now() - start_time).total_seconds()
            
            # 估計剩餘時間
            if processed_rows > 0 and est_total_rows > processed_rows:
                avg_time_per_row = total_time / processed_rows
                est_remaining_time = avg_time_per_row * (est_total_rows - processed_rows)
                est_remaining_min = est_remaining_time / 60
                
                print(f"已處理：{processed_rows:,} 筆 (估計進度: {processed_rows/est_total_rows*100:.1f}%)")
                print(f"本批處理時間：{chunk_time:.1f} 秒 (每秒約 {len(processed_chunk)/chunk_time:.0f} 筆)")
                print(f"累計處理時間：{total_time:.1f} 秒 (估計剩餘: {est_remaining_min:.1f} 分鐘)")
            else:
                print(f"已處理：{processed_rows:,} 筆")
                print(f"本批處理時間：{chunk_time:.1f} 秒 (每秒約 {len(processed_chunk)/chunk_time:.0f} 筆)")
                print(f"累計處理時間：{total_time:.1f} 秒")
            
            if chunk_num == 1:
                print("\n處理後資料欄位:")
                print(processed_chunk.columns.tolist())
                print("\n處理後資料預覽:")
                print(processed_chunk.head())
            
            # 釋放處理後的資料記憶體
            del processed_chunk
    
    except Exception as e:
        print(f"\n處理資料時發生錯誤：")
        print(traceback.format_exc())
        print("\n請檢查錯誤訊息並修改程式碼。")
        return
        
    # 顯示最終資訊
    total_time = (datetime.now() - start_time).total_seconds()
    output_size = os.path.getsize(output_file) / (1024 * 1024)  # 轉換為 MB
    
    print("\n處理完成！")
    print(f"處理模式：CPU處理")
    print(f"總處理時間：{total_time:.1f} 秒")
    print(f"處理總筆數：{processed_rows:,} 筆")
    print(f"處理速度：{processed_rows/total_time:.1f} 筆/秒")
    print(f"輸出檔案：{output_file}")
    print(f"檔案大小：{output_size:.1f} MB")

if __name__ == "__main__":
    main()