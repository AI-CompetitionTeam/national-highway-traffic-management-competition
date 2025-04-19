import json
import pandas as pd
from datetime import datetime
import os


def extract_highway5_precipitation_data(file_path):
    """Extract precipitation data relevant to National Highway 5 from CWA JSON file for the year 2024"""

    print(f"Reading data from: {file_path}")

    try:
        # Read the JSON file
        with open(file_path, 'r', encoding='utf-8') as file:
            weather_data = json.load(file)

        # Highway 5 relevant locations
        highway5_stations = {
            '臺北': ['臺北', '台北', 'Taipei'],
            '新北': ['新北', '板橋', 'New Taipei', 'Banqiao'],
            '宜蘭': ['宜蘭', 'Yilan'],
            '頭城': ['頭城', 'Toucheng'],
            '礁溪': ['礁溪', 'Jiaoxi'],
            '羅東': ['羅東', 'Luodong'],
            '蘇澳': ['蘇澳', 'Su-ao'],
            '坪林': ['坪林', 'Pinglin'],
            '石碇': ['石碇', 'Shiding']
        }

        # Flatten station names for filtering
        all_station_names = []
        for names in highway5_stations.values():
            all_station_names.extend(names)

        # Create lists to store data
        all_precipitation_data = []

        # Navigate to the location data
        locations = weather_data['cwaopendata']['resources']['resource']['data']['surfaceObs']['location']

        # Process each location
        for location in locations:
            station_info = location['station']
            station_name = station_info['StationName']
            station_name_en = station_info['StationNameEN']
            station_id = station_info['StationID']

            # Check if this station is relevant to Highway 5
            is_relevant = False
            for name in all_station_names:
                if name in station_name or name in station_name_en:
                    is_relevant = True
                    break

            if is_relevant:
                # Process observation times for this station
                for obs in location['stationObsTimes']['stationObsTime']:
                    data_time = obs['DataTime']
                    date_obj = datetime.fromisoformat(data_time.replace('+08:00', '+08:00'))

                    # Only process data for the year 2024
                    if date_obj.year != 2024:
                        continue

                    # Extract precipitation data
                    precipitation = obs['weatherElements']['Precipitation']

                    # Handle special values
                    if precipitation == 'T':  # Trace amount
                        precipitation_value = 0.1
                    elif precipitation == 'X':  # Malfunction
                        precipitation_value = None
                    elif precipitation is None:  # No observation
                        precipitation_value = None
                    else:
                        try:
                            precipitation_value = float(precipitation)
                        except (ValueError, TypeError):
                            precipitation_value = None

                    # Only include records with valid precipitation data and positive values
                    if precipitation_value is not None and precipitation_value > 0:
                        all_precipitation_data.append({
                            '觀測站ID': station_id,
                            '觀測站': station_name,
                            '觀測站英文': station_name_en,
                            '日期': date_obj.date(),
                            '時間': date_obj.time(),
                            '年份': date_obj.year,
                            '月份': date_obj.month,
                            '日': date_obj.day,
                            '時': date_obj.hour,
                            '雨量(mm)': precipitation_value,
                            '原始值': precipitation
                        })

        # Convert to DataFrame
        df = pd.DataFrame(all_precipitation_data)

        if df.empty:
            print("No precipitation data found for Highway 5 relevant stations.")
            return None, None

        # Create output directory if it doesn't exist
        output_dir = "國道五號雨量資料_2024"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save detailed hourly data
        timestamp = datetime.now().strftime("%Y%m%d")
        hourly_filename = f"{output_dir}/國道五號雨量資料_逐時_{timestamp}.csv"
        df.to_csv(hourly_filename, index=False, encoding='utf-8-sig')
        print(f"Hourly precipitation data saved to {hourly_filename}")

        # Create daily summary
        daily_summary = df.groupby(['觀測站', '年份', '月份', '日'])['雨量(mm)'].sum().reset_index()
        daily_summary.columns = ['觀測站', '年份', '月份', '日', '日累積雨量(mm)']

        daily_filename = f"{output_dir}/國道五號雨量資料_逐日_{timestamp}.csv"
        daily_summary.to_csv(daily_filename, index=False, encoding='utf-8-sig')
        print(f"Daily precipitation summary saved to {daily_filename}")

        # Create monthly summary
        monthly_summary = daily_summary.groupby(['觀測站', '年份', '月份'])['日累積雨量(mm)'].agg(
            ['sum', 'count', 'mean', 'max']).reset_index()
        monthly_summary.columns = ['觀測站', '年份', '月份', '月累積雨量(mm)', '降雨日數', '平均每日雨量(mm)',
                                   '單日最大雨量(mm)']

        monthly_filename = f"{output_dir}/國道五號雨量資料_月統計_{timestamp}.csv"
        monthly_summary.to_csv(monthly_filename, index=False, encoding='utf-8-sig')
        print(f"Monthly precipitation summary saved to {monthly_filename}")

        return df, monthly_summary

    except Exception as e:
        print(f"Error processing data: {e}")
        return None, None


# Execute data extraction
file_path = r"C:\Users\謝向嶸\Downloads\C-B0024-002.json"
hourly_data, monthly_summary = extract_highway5_precipitation_data(file_path)

if monthly_summary is not None:
    print("\n2024年國道五號相關觀測站降雨月度統計：")
    print(monthly_summary.sort_values(['觀測站', '年份', '月份']))