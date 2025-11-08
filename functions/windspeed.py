import requests
import json
from util.config import env

# 風向度數轉換成方位
def degree_to_direction(degree):
    """
    將風向度數轉換為方位
    0° = 北, 90° = 東, 180° = 南, 270° = 西
    """
    if degree is None or degree == '-99':
        return None
    
    try:
        degree = float(degree)
    except:
        return None
    
    # 16方位
    directions = [
        "北", "北北東", "東北", "東北東",
        "東", "東南東", "東南", "南南東",
        "南", "南南西", "西南", "西南西",
        "西", "西北西", "西北", "北北西"
    ]
    
    # 每個方位佔22.5度 (360/16)
    index = int((degree + 11.25) / 22.5) % 16
    return directions[index]

def windspeed_taipei():
    # API URL
    url = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0001-001"
    params = {
        "Authorization": env.CWA_API_KEY,
        "downloadType": "WEB",
        "format": "JSON"
    }

    # 發送 GET 請求
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        
        # 取得所有測站資料
        dataset = data['cwaopendata']['dataset']
        locations = dataset['Station']
        
        print(f"共有 {len(locations)} 個測站")
        print("\n" + "="*150)
        print(f"{'測站名稱':<12} {'測站ID':<10} {'天氣':<8} {'風速':<8} {'風向':<8} {'氣溫':<8} {'濕度%':<8} {'緯度':<12} {'經度':<12} {'地址':<15}")
        print("="*150)
        
        # 提取台北市的風速資料
        taipei_windspeed = []
        for location in locations:
            station_name = location['StationName']
            station_id = location['StationId']
            county = location['GeoInfo']['CountyName']
            town = location['GeoInfo'].get('TownName', '')
            
            # 只篩選台北市
            if county == '臺北市':
                weather_element = location['WeatherElement']
                weather = weather_element.get('Weather', 'N/A')
                wind_speed = weather_element['WindSpeed']
                wind_direction = weather_element['WindDirection']
                air_temp = weather_element['AirTemperature']
                relative_humidity = weather_element.get('RelativeHumidity', '-99')
                
                # 取得經緯度資訊 (使用 WGS84 座標系統)
                coordinates = location['GeoInfo']['Coordinates']
                latitude = None
                longitude = None
                
                # 尋找 WGS84 座標系統的經緯度
                for coord in coordinates:
                    if coord['CoordinateName'] == 'WGS84':
                        latitude = coord['StationLatitude']
                        longitude = coord['StationLongitude']
                        break
                
                # 過濾掉無效資料 (-99)
                if wind_speed != '-99':
                    # 轉換風向為方位
                    wind_direction_text = degree_to_direction(wind_direction)
                    
                    taipei_windspeed.append({
                        'station_name': station_name,
                        'station_id': station_id,
                        'county': county,
                        'town': town,
                        'latitude': float(latitude) if latitude else None,
                        'longitude': float(longitude) if longitude else None,
                        'weather': weather if weather != '-99' else None,
                        'wind_speed': float(wind_speed),
                        'wind_direction_degree': float(wind_direction) if wind_direction != '-99' else None,
                        'wind_direction': wind_direction_text,
                        'air_temperature': float(air_temp) if air_temp != '-99' else None,
                        'relative_humidity': int(relative_humidity) if relative_humidity != '-99' else None
                    })
                    
                    lat_str = f"{latitude}" if latitude else "N/A"
                    lon_str = f"{longitude}" if longitude else "N/A"
                    weather_str = weather if weather else "N/A"
                    temp_str = f"{air_temp}°C" if air_temp != '-99' else "N/A"
                    humidity_str = f"{relative_humidity}%" if relative_humidity != '-99' else "N/A"
                    wind_dir_str = f"{wind_direction_text}({wind_direction}°)" if wind_direction != '-99' and wind_direction_text else "N/A"
                    print(f"{station_name:<12} {station_id:<10} {weather_str:<8} {wind_speed:<8} {wind_dir_str:<12} {temp_str:<8} {humidity_str:<8} {lat_str:<12} {lon_str:<12} {county}{town:<10}")
        
        print("="*150)
        print(f"\n台北市有效資料筆數: {len(taipei_windspeed)}")
        
        if len(taipei_windspeed) > 0:
            # 計算統計資訊
            wind_speeds = [d['wind_speed'] for d in taipei_windspeed]
            print(f"平均風速: {sum(wind_speeds)/len(wind_speeds):.2f} m/s")
            print(f"最大風速: {max(wind_speeds):.2f} m/s")
            print(f"最小風速: {min(wind_speeds):.2f} m/s")
            
            # 找出風速最大的測站
            max_station = max(taipei_windspeed, key=lambda x: x['wind_speed'])
            print(f"\n風速最大測站: {max_station['station_name']} ({max_station['town']}) - {max_station['wind_speed']} m/s")
            
            # 找出風速最小的測站
            min_station = min(taipei_windspeed, key=lambda x: x['wind_speed'])
            print(f"風速最小測站: {min_station['station_name']} ({min_station['town']}) - {min_station['wind_speed']} m/s")
            
            # 按風速排序
            taipei_windspeed_sorted = sorted(taipei_windspeed, key=lambda x: x['wind_speed'], reverse=True)
            
            print(f"\n台北市各測站風速排名 (由高到低):")
            print("-" * 150)
            for i, station in enumerate(taipei_windspeed_sorted, 1):
                lat = f"{station['latitude']:.6f}" if station['latitude'] else "N/A"
                lon = f"{station['longitude']:.6f}" if station['longitude'] else "N/A"
                weather = station.get('weather', 'N/A')
                temp = f"{station['air_temperature']:.1f}°C" if station.get('air_temperature') else "N/A"
                humidity = f"{station['relative_humidity']}%" if station.get('relative_humidity') else "N/A"
                wind_dir_text = station.get('wind_direction', '')
                wind_dir_degree = station.get('wind_direction_degree')
                wind_dir = f"{wind_dir_text}({wind_dir_degree:.0f}°)" if wind_dir_text and wind_dir_degree else "N/A"
                print(f"{i:2d}. {station['station_name']:<12} {station['town']:<10} {weather:<8} 風速:{station['wind_speed']:>4.1f}m/s 風向:{wind_dir:<15} 溫度:{temp:<8} 濕度:{humidity:<6} 緯度:{lat:<11} 經度:{lon:<11}")
            
            # 儲存為 JSON
            with open('taipei_windspeed.json', 'w', encoding='utf-8') as f:
                json.dump(taipei_windspeed_sorted, f, ensure_ascii=False, indent=2)
            
            print("\n✓ 資料已儲存至 taipei_windspeed.json")
        return taipei_windspeed
    else:
        print(f"HTTP 請求失敗,狀態碼:{response.status_code}")
