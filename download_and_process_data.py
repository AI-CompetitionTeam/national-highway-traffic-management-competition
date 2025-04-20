import os
import requests
import tarfile
import pandas as pd
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 設置重試機制
def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# 創建數據文件夾
os.makedirs('data', exist_ok=True)

# 設定數據來源URL
base_url = "https://tisvcloud.freeway.gov.tw/history/TDCS/M06A/"

# 設定下載日期範圍 - 2024年1月1日至當前日期
start_date = datetime(2024, 1, 1)
end_date = datetime.now() - timedelta(days=5)  # 通常最近的幾天數據可能尚未發布，保留5天緩衝
current_date = start_date

session = get_session()

# 下載數據
while current_date <= end_date:
    date_str = current_date.strftime('%Y%m%d')
    file_name = f"M06A_{date_str}.tar.gz"
    url = f"{base_url}{file_name}"
    
    file_path = os.path.join('data', file_name)
    if not os.path.exists(file_path):
        try:
            print(f"正在下載: {file_name}")
            response = session.get(url, stream=True, timeout=30)
            
            if response.status_code == 200:
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)
                print(f"下載完成: {file_name}")
            else:
                print(f"無法下載 {file_name}: 狀態碼 {response.status_code}")
        except Exception as e:
            print(f"下載 {file_name} 時發生錯誤: {str(e)}")
        
        # 添加延遲以避免短時間內發送太多請求
        time.sleep(2)
    else:
        print(f"檔案已存在，跳過: {file_name}")
    
    current_date += timedelta(days=1)

# 解壓縮並合併CSV檔案
print("開始解壓縮和合併檔案...")
yearly_data = []

# 處理資料夾中的所有.tar.gz檔案
for file_name in sorted(os.listdir('data')):
    if file_name.endswith('.tar.gz') and file_name.startswith("M06A_2024"):
        file_path = os.path.join('data', file_name)
        
        try:
            print(f"正在解壓縮: {file_name}")
            with tarfile.open(file_path, 'r:gz') as tar:
                # 假設每個.tar.gz中只有一個CSV檔案
                for member in tar.getmembers():
                    if member.name.endswith('.csv'):
                        f = tar.extractfile(member)
                        if f:
                            df = pd.read_csv(f)
                            yearly_data.append(df)
        except Exception as e:
            print(f"處理 {file_name} 時發生錯誤: {str(e)}")

# 合併年度資料並儲存
if yearly_data:
    year_df = pd.concat(yearly_data, ignore_index=True)
    year_df.to_csv('data/2024_M06A.csv', index=False)
    print(f"2024年資料已合併為 2024_M06A.csv")

print("處理完成！")