import requests
import json
from typing import Dict, List, Any
from datetime import datetime

from util.config import env


def format_time_key(time_str: str) -> str:
    """
    將 ISO 格式時間轉換為 YYYYMMDDTHHmmSS 格式
    例如: "2025-11-09T00:00:00+08:00" -> "20251109T000000"
    """
    # 解析 ISO 格式時間
    dt = datetime.fromisoformat(time_str)
    # 格式化為 YYYYMMDDTHHmmSS
    return dt.strftime("%Y%m%dT%H%M%S")


def windspeed_taipei_future():
    """
    從中央氣象局 API 獲取台北市未來天氣預報資料並整理成易讀的格式
    
    返回格式:
    {
        "區域名稱": {
            "時間點": {
                "天氣預報": str,
                "風向": str,
                "3小時降雨機率": str,
                "溫度": str,
                "風速": str,
                "天氣現象": str,
                "相對濕度": str,
                "體感溫度": str
            }
        }
    }
    """
    # API URL
    url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-D0047-061"
    params = {
        "Authorization": env.CWA_API_KEY,
        "format": "JSON"
    }

    # 發送 GET 請求
    response = requests.get(url, params=params)

    if response.status_code != 200:
        return {"error": "無法取得資料"}

    data = response.json()['records']['Locations'][0]['Location']
    
    # 整理後的結果
    result = {}
    
    for location in data:
        location_name = location['LocationName']
        result[location_name] = {
            '經緯度': {
                '緯度': location.get('Latitude', 'N/A'),
                '經度': location.get('Longitude', 'N/A'),
                '地理編碼': location.get('Geocode', 'N/A')
            },
            '預報資料': {}
        }
        
        # 取得所有天氣元素
        weather_elements = location['WeatherElement']
        
        # 建立元素名稱對應的資料字典
        elements_data = {}
        for element in weather_elements:
            element_name = element['ElementName']
            elements_data[element_name] = element['Time']
        
        # 以 3 小時為單位的資料(使用有 StartTime 的元素,如「天氣預報綜合描述」作為基準)
        # 這些元素是每 3 小時一筆的完整資料
        if '天氣預報綜合描述' in elements_data:
            for time_entry in elements_data['天氣預報綜合描述']:
                original_time = time_entry['StartTime']
                time_key = format_time_key(original_time)  # 格式化時間
                
                # 初始化該時間點的資料
                result[location_name]['預報資料'][time_key] = {}
                
                # 天氣預報綜合描述
                result[location_name]['預報資料'][time_key]['天氣預報'] = time_entry['ElementValue'][0].get('WeatherDescription', 'N/A')
        
        # 處理其他元素,只保留與基準時間點匹配的資料
        for element_name, time_list in elements_data.items():
            if element_name == '天氣預報綜合描述':
                continue
                
            for time_entry in time_list:
                # 有些元素使用 DataTime, 有些使用 StartTime
                original_time = time_entry.get('DataTime') or time_entry.get('StartTime')
                time_key = format_time_key(original_time)  # 格式化時間
                
                # 只保留已經存在的時間點(即 3 小時間隔的時間點)
                if time_key not in result[location_name]['預報資料']:
                    continue
                
                # 根據不同的元素類型提取資料
                if element_name == '溫度':
                    result[location_name]['預報資料'][time_key]['溫度'] = time_entry['ElementValue'][0].get('Temperature', 'N/A') + '°C'
                
                elif element_name == '風向':
                    result[location_name]['預報資料'][time_key]['風向'] = time_entry['ElementValue'][0].get('WindDirection', 'N/A')
                
                elif element_name == '風速':
                    wind_speed = time_entry['ElementValue'][0].get('WindSpeed', 'N/A')
                    beaufort = time_entry['ElementValue'][0].get('BeaufortScale', 'N/A')
                    result[location_name]['預報資料'][time_key]['風速'] = f"{wind_speed} m/s (蒲福{beaufort}級)"
                
                elif element_name == '3小時降雨機率':
                    result[location_name]['預報資料'][time_key]['3小時降雨機率'] = time_entry['ElementValue'][0].get('ProbabilityOfPrecipitation', 'N/A') + '%'
                
                elif element_name == '天氣現象':
                    weather = time_entry['ElementValue'][0].get('Weather', 'N/A')
                    weather_code = time_entry['ElementValue'][0].get('WeatherCode', 'N/A')
                    result[location_name]['預報資料'][time_key]['天氣現象'] = f"{weather} (代碼:{weather_code})"
                
                elif element_name == '相對濕度':
                    result[location_name]['預報資料'][time_key]['相對濕度'] = time_entry['ElementValue'][0].get('RelativeHumidity', 'N/A') + '%'
                
                elif element_name == '體感溫度':
                    result[location_name]['預報資料'][time_key]['體感溫度'] = time_entry['ElementValue'][0].get('ApparentTemperature', 'N/A') + '°C'
    
    return result


def print_weather_data(weather_data: dict, location: str = None, limit: int = 5):
    """
    列印天氣資料
    
    Args:
        weather_data: 由 windspeed_taipei_future() 返回的資料
        location: 指定區域名稱,若為 None 則顯示所有區域
        limit: 限制顯示的時間點數量
    """
    if not weather_data or "error" in weather_data:
        print("無法取得天氣資料")
        return
    
    locations_to_print = [location] if location else list(weather_data.keys())
    
    for loc in locations_to_print:
        if loc not in weather_data:
            print(f"找不到區域: {loc}")
            continue
            
        print(f"\n{'='*60}")
        print(f"區域: {loc}")
        print(f"{'='*60}")
        
        # 取得前 N 個時間點
        time_keys = sorted(list(weather_data[loc].keys()))[:limit]
        
        for time_key in time_keys:
            data = weather_data[loc][time_key]
            print(f"\n時間: {time_key}")
            print("-" * 60)
            
            # 依照指定順序顯示
            keys_order = ['天氣預報', '風向', '3小時降雨機率', '溫度', '風速', '天氣現象', '相對濕度', '體感溫度']
            
            for key in keys_order:
                if key in data:
                    # 天氣預報內容較長,特別處理
                    if key == '天氣預報':
                        print(f"{key}: {data[key][:80]}..." if len(data[key]) > 80 else f"{key}: {data[key]}")
                    else:
                        print(f"{key}: {data[key]}")
        
        print("\n")

        
if __name__ == "__main__":
    # 獲取資料
    weather_data = windspeed_taipei_future()
    
    # 列印特定區域的前5筆資料
    print_weather_data(weather_data, location="信義區")
    
    # 或者將資料儲存為 JSON 檔案
    with open('taipei_weather.json', 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    print("完整資料已儲存至 taipei_weather.json")