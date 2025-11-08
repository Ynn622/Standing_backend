from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import asyncio
from datetime import datetime
import json
import h3
from pathlib import Path
from datetime import datetime
import asyncio

router = APIRouter(prefix="/map", tags=["Map"])

# 快取變數 - 使用永久快取，除非手動清除
_road_risk_cache = None
_road_risk_cache_time = None
_hexgrid_cache = None  # 快取六角格資料
_roads_cache = None    # 快取道路資料

# === 道路風險分析輔助函數 ===

def _fetch_roads_from_overpass():
    """從 Overpass API 抓取台北市道路資料並儲存"""
    import requests
    import time
    
    print("📡 正在從 Overpass API 抓取道路資料...")
    
    # Overpass API endpoint
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Overpass QL 查詢語法
    # 取得台北市範圍內的所有道路類型（包含巷弄、小路等）
    overpass_query = """
    [out:json][timeout:180];
    (
      way["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|tertiary|tertiary_link|residential|living_street|service|unclassified|road"]
        (24.95,121.40,25.20,121.70);
    );
    out geom;
    """
    
    try:
        response = requests.post(
            overpass_url,
            data={'data': overpass_query},
            timeout=240
        )
        
        if response.status_code == 200:
            data = response.json()
            roads = data.get('elements', [])
            
            print(f"✅ 成功取得 {len(roads)} 條道路")
            
            # 處理道路資料
            processed_roads = []
            for road in roads:
                if 'geometry' in road:
                    road_data = {
                        'id': road.get('id'),
                        'type': road.get('tags', {}).get('highway', 'unknown'),
                        'name': road.get('tags', {}).get('name', '未命名道路'),
                        'geometry': [
                            {'lat': point['lat'], 'lng': point['lon']} 
                            for point in road['geometry']
                        ]
                    }
                    processed_roads.append(road_data)
            
            # 儲存到 dataStore
            roads_file = Path(__file__).parent.parent / 'dataStore' / 'taipei_roads.json'
            roads_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(roads_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'total_roads': len(processed_roads),
                    'roads': processed_roads
                }, f, ensure_ascii=False, indent=2)
            
            print(f"💾 已儲存至 {roads_file}")
            return processed_roads
            
        else:
            raise Exception(f"HTTP {response.status_code}: {response.text[:200]}")
            
    except Exception as e:
        raise Exception(f"從 Overpass API 抓取資料失敗: {str(e)}")

def _load_road_analysis_data():
    """載入六角格資料和道路資料（使用快取）"""
    global _hexgrid_cache, _roads_cache
    
    # 如果已經快取，直接返回
    if _hexgrid_cache is not None and _roads_cache is not None:
        return _hexgrid_cache, _roads_cache
    
    # 資料檔案路徑
    hexgrid_file = Path(__file__).parent.parent / 'dataStore' / 'hexgrid_data.json'
    roads_file = Path(__file__).parent.parent / 'dataStore' / 'taipei_roads.json'
    
    # 檢查六角格檔案是否存在
    if not hexgrid_file.exists():
        raise FileNotFoundError(f"六角格資料檔案不存在: {hexgrid_file}")
    
    # 檢查道路檔案，如果不存在則從 Overpass API 抓取
    if not roads_file.exists():
        print(f"⚠️ 道路資料檔案不存在，正在從 Overpass API 抓取...")
        try:
            roads_data = _fetch_roads_from_overpass()
        except Exception as e:
            raise FileNotFoundError(f"道路資料檔案不存在且無法從 Overpass API 抓取: {str(e)}")
    
    # 讀取六角格資料
    with open(hexgrid_file, 'r', encoding='utf-8') as f:
        hexgrid_data = json.load(f)
    
    # 建立 H3 索引對應表（注意：資料在 resolutions.res_10.hexagons 裡）
    if 'resolutions' in hexgrid_data and 'res_10' in hexgrid_data['resolutions']:
        hexagons = hexgrid_data['resolutions']['res_10']['hexagons']
        h3_map = {
            item['h3_index']: item['combined_value'] 
            for item in hexagons
        }
    else:
        # 舊格式：直接是陣列
        h3_map = {
            item['h3_index']: item['combined_value'] 
            for item in hexgrid_data
        }
    
    # 讀取道路資料
    with open(roads_file, 'r', encoding='utf-8') as f:
        roads_data = json.load(f)
    
    # 快取資料
    _hexgrid_cache = h3_map
    _roads_cache = roads_data['roads']
    
    return h3_map, roads_data['roads']

# ============ 輔助函數：道路風險計算 ============

def _calculate_road_value(geometry, h3_map):
    """計算道路的平均組合值"""
    total_value = 0
    valid_points = 0
    
    sample_count = min(10, len(geometry))
    sample_interval = max(1, len(geometry) // sample_count)
    
    for i in range(0, len(geometry), sample_interval):
        point = geometry[i]
        try:
            h3_index = h3.latlng_to_cell(point['lat'], point['lng'], 10)
            if h3_index in h3_map:
                total_value += h3_map[h3_index]
                valid_points += 1
        except:
            pass
    
    return total_value / valid_points if valid_points > 0 else None

def _get_risk_level(value):
    """根據組合值判斷風險等級和顏色"""
    if value is None:
        return {
            'level': 'unknown',
            'level_name': '未知',
            'color': 'gray',
            'color_rgb': 'rgb(128, 128, 128)'
        }
    
    if value < 10.8:
        return {
            'level': 1,
            'level_name': '極低風險',
            'color': 'green',
            'color_rgb': 'rgb(26, 152, 80)'
        }
    elif value < 12.5:
        return {
            'level': 2,
            'level_name': '低風險',
            'color': 'light_green',
            'color_rgb': 'rgb(166, 217, 106)'
        }
    elif value < 14.4:
        return {
            'level': 3,
            'level_name': '中風險',
            'color': 'yellow',
            'color_rgb': 'rgb(255, 255, 0)'
        }
    elif value < 16.2:
        return {
            'level': 4,
            'level_name': '高風險',
            'color': 'orange',
            'color_rgb': 'rgb(253, 174, 97)'
        }
    else:
        return {
            'level': 5,
            'level_name': '極高風險',
            'color': 'red',
            'color_rgb': 'rgb(215, 25, 28)'
        }

def _analyze_roads_task(h3_map, roads):
    """分析所有道路的風險等級"""
    analyzed_roads = []
    stats = {
        1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 'unknown': 0
    }
    
    for road in roads:
        # 跳過未命名道路
        name = road.get('name', '未命名道路')
        if name == '未命名道路':
            continue
        
        # 確保 geometry 存在
        if 'geometry' not in road or not road['geometry']:
            continue
            
        combined_value = _calculate_road_value(road['geometry'], h3_map)
        risk_info = _get_risk_level(combined_value)
        
        if risk_info['level'] == 'unknown':
            stats['unknown'] += 1
        else:
            stats[risk_info['level']] += 1
        
        road_info = {
            'id': road['id'],
            'name': name,
            'type': road.get('type', 'unknown'),
            'combined_value': round(combined_value, 2) if combined_value is not None else None,
            'risk_level': risk_info['level'],
            'risk_level_name': risk_info['level_name'],
            'color': risk_info['color'],
            'color_rgb': risk_info['color_rgb'],
            'start_point': road['geometry'][0],
            'end_point': road['geometry'][-1],
            'geometry_point_count': len(road['geometry'])
        }
        analyzed_roads.append(road_info)
    
    return analyzed_roads, stats

def _prepare_result_dict(analyzed_roads, stats):
    """準備結果字典"""
    result_dict = {}
    
    for level in range(1, 6):
        level_roads = [r for r in analyzed_roads 
                      if r['risk_level'] == level and r['name'] != '未命名道路']
        
        simplified_level_roads = []
        for road in level_roads:
            simplified_level_roads.append({
                'name': road['name'],
                'start': road['start_point'],
                'end': road['end_point']
            })
        
        result_dict[f'level_{level}'] = {
            'risk_level': level,
            'risk_level_name': level_roads[0]['risk_level_name'] if level_roads else '',
            'count': len(level_roads),
            'roads': simplified_level_roads
        }
    
    return result_dict

# ============ 原有函數 ============


def _update_hexgrid_task():
    """背景任務：更新六角格資料"""
    from functions.mapData_proccess import TaipeiDataManager
    manager = TaipeiDataManager(api_key="rdec-key-123-45678-011121314")
    return manager.update_hexgrid_data(resolutions=[10])


@router.get("/update_hexgrid_data", response_class=JSONResponse)
async def update_hexgrid_data(background: bool = False, background_tasks: BackgroundTasks = None):
    """
    更新六角格區塊資料
    計算建築高度與風速的組合值，生成 H3 六角形網格資料
    只包含解析度 10 (直徑約 76m)
    
    參數:
    - background: 是否在背景執行 (預設: False)
      - True: 立即返回，資料在背景更新
      - False: 等待更新完成後返回結果
    """
    try:
        if background and background_tasks:
            # 背景執行模式
            background_tasks.add_task(_update_hexgrid_task)
            return {
                "success": True,
                "message": "六角格資料更新已啟動（背景執行）",
                "data": {
                    "status": "processing",
                    "started_at": datetime.now().isoformat()
                }
            }
        else:
            # 同步執行模式
            from functions.mapData_proccess import TaipeiDataManager
            
            # 創建管理器實例
            manager = TaipeiDataManager(api_key="rdec-key-123-45678-011121314")
            
            # 在背景執行更新（使用 asyncio）
            loop = asyncio.get_event_loop()
            hexgrid_data = await loop.run_in_executor(
                None,
                lambda: manager.update_hexgrid_data(resolutions=[10])
            )
            
            # 返回成功訊息
            return {
                "success": True,
                "message": "六角格資料更新成功",
                "data": {
                    "total_buildings": hexgrid_data['metadata']['total_buildings'],
                    "total_weather_stations": hexgrid_data['metadata']['total_weather_stations'],
                    "update_time": hexgrid_data['metadata']['update_time'],
                    "resolutions": {
                        "res_10": hexgrid_data['resolutions']['res_10']['total_hexagons']
                    },
                    "file_saved": "hexgrid_data.json"
                }
            }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"更新六角格資料時發生錯誤: {str(e)}"
        )

@router.get("/analyze_road_risk", response_class=JSONResponse)
async def analyze_road_risk(risk_level: int = None, use_cache: bool = True):
    """
    分析道路風險等級
    
    參數:
    - risk_level: 指定風險等級 (1-5)，不指定則返回所有等級
      - 1: 極低風險 (< 10.8)
      - 2: 低風險 (10.8-12.5)
      - 3: 中風險 (12.5-14.4)
      - 4: 高風險 (14.4-16.2)
      - 5: 極高風險 (≥ 16.2)
    - use_cache: 是否使用快取 (預設: True)
    
    返回:
    - 道路風險分析結果（字典格式）
    """
    global _road_risk_cache, _road_risk_cache_time
    
    try:
        # 檢查快取（永久有效，除非 use_cache=False）
        if use_cache and _road_risk_cache is not None:
            result_dict = _road_risk_cache
            stats = _road_risk_cache.get('_stats', {})
            total_roads = _road_risk_cache.get('_total_roads', 0)
            
            # 如果指定了風險等級，只返回該等級
            if risk_level is not None:
                if risk_level < 1 or risk_level > 5:
                    raise HTTPException(
                        status_code=400,
                        detail="risk_level 必須在 1-5 之間"
                    )
                
                level_key = f'level_{risk_level}'
                return {
                    "success": True,
                    "message": f"道路風險分析完成 (使用快取) - {result_dict[level_key]['risk_level_name']}",
                    "cached": True,
                    "data": result_dict[level_key],
                    "statistics": {
                        "total_roads_analyzed": total_roads,
                        "level_1_count": stats.get(1, 0),
                        "level_2_count": stats.get(2, 0),
                        "level_3_count": stats.get(3, 0),
                        "level_4_count": stats.get(4, 0),
                        "level_5_count": stats.get(5, 0),
                        "unknown_count": stats.get('unknown', 0)
                    }
                }
            
            # 返回所有等級 (移除內部統計資料)
            clean_result = {k: v for k, v in result_dict.items() if not k.startswith('_')}
            return {
                "success": True,
                "message": "道路風險分析完成 (使用快取)",
                "cached": True,
                "data": clean_result,
                "statistics": {
                    "total_roads_analyzed": total_roads,
                    "level_1_count": stats.get(1, 0),
                    "level_2_count": stats.get(2, 0),
                    "level_3_count": stats.get(3, 0),
                    "level_4_count": stats.get(4, 0),
                    "level_5_count": stats.get(5, 0),
                    "unknown_count": stats.get('unknown', 0)
                }
            }
        
        # 載入資料並分析
        h3_map, roads = _load_road_analysis_data()
        
        # 在背景執行分析
        loop = asyncio.get_event_loop()
        analyzed_roads, stats = await loop.run_in_executor(
            None,
            lambda: _analyze_roads_task(h3_map, roads)
        )
        
        # 準備結果字典
        result_dict = _prepare_result_dict(analyzed_roads, stats)
        
        # 儲存到快取（永久快取）
        result_dict['_stats'] = stats
        result_dict['_total_roads'] = len(roads)
        _road_risk_cache = result_dict
        _road_risk_cache_time = datetime.now()
        
        # 如果指定了風險等級，只返回該等級
        if risk_level is not None:
            if risk_level < 1 or risk_level > 5:
                raise HTTPException(
                    status_code=400,
                    detail="risk_level 必須在 1-5 之間"
                )
            
            level_key = f'level_{risk_level}'
            return {
                "success": True,
                "message": f"道路風險分析完成 - {result_dict[level_key]['risk_level_name']}",
                "cached": False,
                "data": result_dict[level_key],
                "statistics": {
                    "total_roads_analyzed": len(roads),
                    "level_1_count": stats[1],
                    "level_2_count": stats[2],
                    "level_3_count": stats[3],
                    "level_4_count": stats[4],
                    "level_5_count": stats[5],
                    "unknown_count": stats['unknown']
                }
            }
        
        # 返回所有等級 (移除內部統計資料)
        clean_result = {k: v for k, v in result_dict.items() if not k.startswith('_')}
        return {
            "success": True,
            "message": "道路風險分析完成",
            "cached": False,
            "data": clean_result,
            "statistics": {
                "total_roads_analyzed": len(roads),
                "level_1_count": stats[1],
                "level_2_count": stats[2],
                "level_3_count": stats[3],
                "level_4_count": stats[4],
                "level_5_count": stats[5],
                "unknown_count": stats['unknown']
            }
        }
    
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"找不到必要的資料檔案: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"分析道路風險時發生錯誤: {str(e)}"
        )

@router.post("/clear_road_risk_cache", response_class=JSONResponse)
async def clear_road_risk_cache():
    """
    清除道路風險分析的快取
    
    使用時機:
    - 當六角格資料更新時
    - 當道路資料更新時
    - 需要重新計算風險值時
    """
    global _road_risk_cache, _road_risk_cache_time, _hexgrid_cache, _roads_cache
    
    _road_risk_cache = None
    _road_risk_cache_time = None
    _hexgrid_cache = None
    _roads_cache = None
    
    return {
        "success": True,
        "message": "快取已清除，下次調用將重新載入資料並計算"
    }
