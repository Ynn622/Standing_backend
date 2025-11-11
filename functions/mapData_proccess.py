"""
台北建築與氣象資料整合系統
功能：
1. 更新氣象資料 (從中央氣象署 API)
2. 更新區塊資料 (建築高度 + 風速，以 H3 六角形網格儲存)
"""

import requests
import pandas as pd
import json
from datetime import datetime
import h3
from typing import Dict, List, Tuple
import math
import os


class TaipeiDataManager:
    """台北建築與氣象資料管理器"""
    
    def __init__(self, api_key: str = "rdec-key-123-45678-011121314"):
        self.api_key = api_key
        self.weather_api_url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0003-001"
        
        # 取得專案根目錄的絕對路徑
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, "dataStore")
        
        # 設定檔案路徑
        self.buildings_csv = os.path.join(self.data_dir, "taipei_buildings_sample.csv")
        
        # 確保 dataStore 目錄存在
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 定義 TWD97 投影 (EPSG:3826)
        self.proj_twd97 = "+proj=tmerc +lat_0=0 +lon_0=121 +k=0.9999 +x_0=250000 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
        self.proj_wgs84 = "+proj=longlat +datum=WGS84 +no_defs"
    
    def _get_transformer(self):
        """
        取得座標轉換器（快取以提升效能）
        """
        if not hasattr(self, '_transformer'):
            import pyproj
            twd97 = pyproj.CRS("EPSG:3826")
            wgs84 = pyproj.CRS("EPSG:4326")
            self._transformer = pyproj.Transformer.from_crs(twd97, wgs84, always_xy=True)
        return self._transformer
    
    def _convert_twd97_to_wgs84(self, e97: float, n97: float) -> Tuple[float, float]:
        """
        轉換 TWD97 座標到 WGS84 經緯度
        
        Args:
            e97: TWD97 東向座標
            n97: TWD97 北向座標
            
        Returns:
            (lng, lat): 經度, 緯度
        """
        transformer = self._get_transformer()
        lng, lat = transformer.transform(e97, n97)
        return lng, lat
    
    def _fetch_weather_data_from_api(self) -> List[Dict]:
        """
        從 API 直接取得氣象資料（不儲存 CSV）
        
        Returns:
            List[Dict]: 氣象站資料列表
        """
        try:
            # 呼叫 API
            params = {'Authorization': self.api_key}
            response = requests.get(self.weather_api_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # 檢查 API 回應
            if data.get('success') != 'true':
                raise Exception(f"API 回應失敗: {data}")
            
            # 解析氣象站資料
            stations = []
            records = data.get('records', {}).get('Station', [])
            
            for station in records:
                try:
                    # 基本資訊
                    station_id = station.get('StationId', '')
                    station_name = station.get('StationName', '')
                    
                    # 觀測時間
                    obs_time_data = station.get('ObsTime', {})
                    obs_time = obs_time_data.get('DateTime', '') if isinstance(obs_time_data, dict) else ''
                    
                    # 地理位置
                    geo_info = station.get('GeoInfo', {})
                    county = geo_info.get('CountyName', '')
                    town = geo_info.get('TownName', '')
                    
                    # 取得經緯度（使用 WGS84 座標）
                    geocode_list = geo_info.get('Coordinates', [])
                    lat = 0
                    lng = 0
                    
                    for coord in geocode_list:
                        if coord.get('CoordinateName') == 'WGS84':
                            try:
                                lat = float(coord.get('StationLatitude', '0'))
                                lng = float(coord.get('StationLongitude', '0'))
                                break
                            except (ValueError, TypeError):
                                lat = 0
                                lng = 0
                    
                    # 氣象數據
                    weather_elements = station.get('WeatherElement', {})
                    
                    def get_value(key):
                        """安全取得數值"""
                        val = weather_elements.get(key, None)
                        if val is None or val == '' or val == '-99' or val == '-998' or val == '-999':
                            return None
                        try:
                            return float(val)
                        except (ValueError, TypeError):
                            return None
                    
                    # 處理降雨量（特殊結構）
                    precipitation = 0
                    precip_data = weather_elements.get('Now', {})
                    if isinstance(precip_data, dict):
                        precip_val = precip_data.get('Precipitation', 0)
                        try:
                            precipitation = float(precip_val) if precip_val not in [None, '', '-99', '-998'] else 0
                        except (ValueError, TypeError):
                            precipitation = 0
                    
                    station_data = {
                        'station_id': station_id,
                        'station_name': station_name,
                        'county': county,
                        'town': town,
                        'latitude': lat,
                        'longitude': lng,
                        'obs_time': obs_time,
                        'temperature': get_value('AirTemperature'),
                        'humidity': get_value('RelativeHumidity'),
                        'wind_speed': get_value('WindSpeed'),
                        'wind_direction': get_value('WindDirection'),
                        'pressure': get_value('AirPressure'),
                        'precipitation': precipitation,
                        'weather': weather_elements.get('Weather', ''),
                    }
                    
                    # 只保留有效的台北市及新北市測站
                    if lat > 0 and lng > 0 and county in ['臺北市', '新北市', '台北市']:
                        stations.append(station_data)
                        
                except Exception as e:
                    continue
            
            return stations
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API 請求錯誤: {e}")
            return []
        except Exception as e:
            print(f"❌ 處理氣象資料時發生錯誤: {e}")
            return []
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        計算兩點之間的距離（公里）
        使用 Haversine 公式
        """
        R = 6371  # 地球半徑（公里）
        
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def update_hexgrid_data(self, resolutions: List[int] = [10]) -> Dict:
        """
        2. 更新區塊資料
        計算不同解析度的六角形網格資料（建築平均高度 + 最近測站風速）
        
        Args:
            resolutions: H3 解析度列表 [10=小 (直徑約 76m)]

        Returns:
            Dict: 包含所有解析度的區塊資料
        """
        print("🏢 開始更新區塊資料...")

        # 1. 載入建築資料
        print("📂 載入建築資料...")
        buildings_df = pd.read_csv(self.buildings_csv)
        
        # 使用向量化操作清理資料
        print("🔄 轉換座標...")
        buildings_df['CENT_E_97'] = buildings_df['CENT_E_97'].astype(str).str.replace('\x00', '')
        buildings_df['CENT_N_97'] = buildings_df['CENT_N_97'].astype(str).str.replace('\x00', '')
        buildings_df['BUILD_H'] = buildings_df['BUILD_H'].astype(str).str.replace('\x00', '')
        
        # 轉換為數值
        buildings_df['e97'] = pd.to_numeric(buildings_df['CENT_E_97'], errors='coerce')
        buildings_df['n97'] = pd.to_numeric(buildings_df['CENT_N_97'], errors='coerce')
        buildings_df['height'] = pd.to_numeric(buildings_df['BUILD_H'], errors='coerce')
        
        # 移除無效資料
        buildings_df = buildings_df.dropna(subset=['e97', 'n97', 'height'])
        
        # 批次轉換座標（使用快取的 transformer）
        transformer = self._get_transformer()
        coords = transformer.transform(
            buildings_df['e97'].values,
            buildings_df['n97'].values
        )
        
        buildings_df['lng'] = coords[0]
        buildings_df['lat'] = coords[1]
        
        # 轉換為字典列表
        buildings = buildings_df[['lat', 'lng', 'height']].to_dict('records')
        
        print(f"✅ 載入 {len(buildings)} 筆建築資料")
        
        # 2. 從 API 直接載入氣象資料
        print("📂 從 API 載入氣象資料...")
        weather_stations = self._fetch_weather_data_from_api()
        print(f"✅ 載入 {len(weather_stations)} 個氣象站資料")
        
        # 3. 計算各解析度的六角形統計
        result = {
            'metadata': {
                'update_time': datetime.now().isoformat(),
                'total_buildings': len(buildings),
                'total_weather_stations': len(weather_stations),
            },
            'resolutions': {}
        }
        
        resolution_names = {
            10: '小 (直徑約 76m)'
        }
        
        for resolution in resolutions:
            print(f"\n🔷 計算解析度 {resolution} - {resolution_names.get(resolution, '')}...")
            
            # 定義台北市的範圍（擴大範圍以確保完整覆蓋）
            taipei_bounds = {
                'min_lat': 24.95,   # 南界（擴大）
                'max_lat': 25.20,   # 北界（擴大）
                'min_lng': 121.40,  # 西界（擴大）
                'max_lng': 121.70   # 東界（擴大）
            }
            
            # 計算中心點
            center_lat = (taipei_bounds['min_lat'] + taipei_bounds['max_lat']) / 2
            center_lng = (taipei_bounds['min_lng'] + taipei_bounds['max_lng']) / 2
            center_hex = h3.latlng_to_cell(center_lat, center_lng, resolution)
            
            # 根據解析度調整半徑，確保覆蓋整個台北市
            radius_map = {
                10: 100
            }
            radius = radius_map.get(resolution, 100)
            
            print(f"   📍 中心點: ({center_lat:.4f}, {center_lng:.4f})")
            print(f"   📏 生成半徑: {radius} 個六角形")
            
            # 使用 gridDisk 生成覆蓋範圍的所有六角形
            all_hexagons = h3.grid_disk(center_hex, radius)
            print(f"   🔢 初步生成: {len(all_hexagons)} 個六角形")
            
            # 過濾出在台北市範圍內的六角形
            taipei_hexagons = set()
            for hex_id in all_hexagons:
                lat, lng = h3.cell_to_latlng(hex_id)
                if (taipei_bounds['min_lat'] <= lat <= taipei_bounds['max_lat'] and 
                    taipei_bounds['min_lng'] <= lng <= taipei_bounds['max_lng']):
                    taipei_hexagons.add(hex_id)
            
            print(f"   ✅ 過濾後（台北市範圍內）: {len(taipei_hexagons)} 個六角形")
            
            # 建立六角形統計（先初始化所有六角形）
            hex_stats = {}
            for hex_id in taipei_hexagons:
                lat, lng = h3.cell_to_latlng(hex_id)
                hex_stats[hex_id] = {
                    'heights': [],
                    'center': (lat, lng)
                }
            
            # 為每棟建築分配到六角形
            buildings_assigned = 0
            for building in buildings:
                h3_index = h3.latlng_to_cell(building['lat'], building['lng'], resolution)
                
                # 只處理在台北市範圍內的建築
                if h3_index in hex_stats:
                    hex_stats[h3_index]['heights'].append(building['height'])
                    buildings_assigned += 1
            
            print(f"   🏢 分配建築: {buildings_assigned} / {len(buildings)} 筆")
            
            # 計算每個六角形的平均高度和氣象資料
            hex_data = []
            hexes_with_buildings = 0
            
            for h3_index, stats in hex_stats.items():
                lat, lng = stats['center']
                
                # 計算平均高度（如果有建築物）
                if len(stats['heights']) > 0:
                    max_height = max(stats['heights'])
                    hexes_with_buildings += 1
                else:
                    max_height = 0
                
                # 找到最近的氣象站及其風速（所有六角形都要）
                nearest_station = None
                min_distance = float('inf')
                
                for station in weather_stations:
                    if station['latitude'] > 0 and station['longitude'] > 0:
                        distance = self._haversine_distance(
                            lat, lng,
                            station['latitude'], station['longitude']
                        )
                        
                        if distance < min_distance:
                            min_distance = distance
                            nearest_station = station
                
                # 計算組合值（最高樓高 + 風速調整公式）
                wind_speed = nearest_station['wind_speed'] if nearest_station and nearest_station.get('wind_speed') else 0
                # 風速調整公式：((wind * (1.5/10)**0.25) * (min(1 + 0.25*max(0, max_height/8 -1), 1.6)) * (1.36))
                # combined_value = max_height + (wind_speed if wind_speed else 0)
                # wind_speed = 11
                combined_value = ((wind_speed * (1.5/10)**0.25) * (min(1 + 0.25*max(0, max_height/8 -1), 1.6)) * (1.36))
                
                # 只保留必要欄位：h3_index, combined_value
                hex_data.append({
                    'h3_index': h3_index,
                    'combined_value': round(combined_value, 2)
                })
            
            print(f"   📊 有建築物的六角形: {hexes_with_buildings} / {len(hex_data)}")
            
            result['resolutions'][f'res_{resolution}'] = {
                'resolution': resolution,
                'description': resolution_names.get(resolution, ''),
                'total_hexagons': len(hex_data),
                'hexagons': hex_data
            }
            
            print(f"   ✅ 生成 {len(hex_data)} 個六角形區塊")
        
        # 4. 儲存為 JSON
        output_file = os.path.join(self.data_dir, 'hexgrid_data.json')
        
        # 確保目錄存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n✅ 區塊資料已更新！")
        print(f"💾 儲存至: {output_file}")
        print(f"📊 總計：")
        for res_key, res_data in result['resolutions'].items():
            print(f"   - 解析度 {res_data['resolution']}: {res_data['total_hexagons']} 個六角形")
        
        return result


def main():
    """主程式"""
    print("=" * 60)
    print("🏙️  台北建築與氣象資料整合系統")
    print("=" * 60)
    print()
    
    # 初始化管理器
    manager = TaipeiDataManager(api_key="rdec-key-123-45678-011121314")
    
    try:
        # 1. 更新氣象資料
        print("\n【步驟 1】更新氣象資料")
        print("-" * 60)
        weather_df = manager.update_weather_data()
        
        # 2. 更新區塊資料（只保留「小」解析度）
        print("\n【步驟 2】更新區塊資料")
        print("-" * 60)
        hexgrid_data = manager.update_hexgrid_data(resolutions=[10])
        
        print("\n" + "=" * 60)
        print("✅ 所有資料更新完成！")
        print("=" * 60)
        
        # 顯示統計摘要
        print("\n📊 資料摘要：")
        print(f"   氣象站數量: {len(weather_df)}")
        print(f"   建築數量: {hexgrid_data['metadata']['total_buildings']}")
        print(f"   更新時間: {hexgrid_data['metadata']['update_time']}")
        
    except Exception as e:
        print(f"\n❌ 執行失敗: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
