import os
import pandas as pd
import tarfile
import shutil
import sys
import traceback

# 取得目前檔案所在的目錄路徑
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, 'data')

# 在 D 槽建立臨時資料夾
temp_dir = 'D:/temp_highway_data'
output_dir = 'D:/highway_output'

print(f"資料來源目錄: {data_dir}")
print(f"臨時資料夾: {temp_dir}")
print(f"輸出目錄: {output_dir}")

try:
    # 建立必要的資料夾
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n開始解壓縮和合併檔案...")
    
    # 檢查資料夾是否存在
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"找不到資料來源目錄: {data_dir}")
    
    # 檢查是否有.tar.gz檔案
    tar_files = [f for f in os.listdir(data_dir) if f.endswith('.tar.gz') and '2024' in f]
    if not tar_files:
        raise FileNotFoundError(f"在 {data_dir} 中找不到2024年的.tar.gz檔案")
    
    print(f"找到 {len(tar_files)} 個.tar.gz檔案")
    
    # 建立輸出檔案
    output_file = os.path.join(output_dir, '2024_M06A.csv')
    header_saved = False
    
    # 處理資料夾中的所有.tar.gz檔案
    for file_name in sorted(tar_files):
        file_path = os.path.join(data_dir, file_name)
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # 轉換為MB
        print(f"\n正在處理: {file_name} (大小: {file_size:.2f} MB)")
        
        try:
            with tarfile.open(file_path, 'r:gz') as tar:
                # 解壓縮到臨時資料夾
                for member in tar.getmembers():
                    if member.name.endswith('.csv'):
                        print(f"  發現CSV檔案: {member.name}")
                        f = tar.extractfile(member)
                        if f:
                            temp_csv = os.path.join(temp_dir, os.path.basename(member.name))
                            print(f"  正在解壓縮到: {temp_csv}")
                            
                            # 將檔案解壓縮到臨時資料夾
                            with open(temp_csv, 'wb') as out:
                                shutil.copyfileobj(f, out)
                            
                            # 檢查解壓縮後的檔案大小
                            csv_size = os.path.getsize(temp_csv) / (1024 * 1024)  # 轉換為MB
                            print(f"  解壓縮後大小: {csv_size:.2f} MB")
                            
                            # 分批讀取CSV並直接寫入輸出檔案
                            chunk_size = 10000  # 每次讀取10000行
                            chunks_processed = 0
                            print("  開始分批處理CSV檔案...")
                            
                            for chunk in pd.read_csv(temp_csv, chunksize=chunk_size):
                                if not header_saved:
                                    chunk.to_csv(output_file, mode='w', index=False)
                                    header_saved = True
                                else:
                                    chunk.to_csv(output_file, mode='a', header=False, index=False)
                                chunks_processed += 1
                                if chunks_processed % 10 == 0:
                                    print(f"    已處理 {chunks_processed} 個批次...")
                            
                            print(f"  完成處理 {chunks_processed} 個批次")
                            
                            # 刪除臨時CSV
                            os.remove(temp_csv)
                            print(f"  已刪除臨時檔案: {temp_csv}")
        except Exception as e:
            print(f"\n處理 {file_name} 時發生錯誤:")
            print(traceback.format_exc())
            continue
    
    # 清理臨時資料夾
    shutil.rmtree(temp_dir)
    print("\n已清理臨時資料夾")
    
    if os.path.exists(output_file):
        output_size = os.path.getsize(output_file) / (1024 * 1024)  # 轉換為MB
        print(f"\n處理完成！")
        print(f"輸出檔案位置：{output_file}")
        print(f"輸出檔案大小：{output_size:.2f} MB")
    else:
        print("\n處理完成，但找不到輸出檔案！")

except Exception as e:
    print("\n程式執行過程中發生錯誤:")
    print(traceback.format_exc()) 