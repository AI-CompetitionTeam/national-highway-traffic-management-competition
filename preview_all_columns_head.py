#可查看100筆資料
import os
import pandas as pd

input_dir = 'D:/highway_output/split_columns'
output_txt = 'national-highway-traffic-management-competition/preview_all_columns_head_output.txt'

files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

with open(output_txt, 'w', encoding='utf-8') as out:
    for file in files:
        file_path = os.path.join(input_dir, file)
        try:
            df = pd.read_csv(file_path, nrows=100)
            out.write(f'\n檔案: {file}\n')
            out.write(df.head(100).to_string(index=False))
            out.write('\n' + '-'*60 + '\n')
        except Exception as e:
            out.write(f'讀取 {file} 發生錯誤：{e}\n') 