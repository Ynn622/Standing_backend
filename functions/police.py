import requests
import json
import pandas as pd
from datetime import datetime

def police_news_data():
    url = "https://rtr.pbs.gov.tw/pbsmgt/RoadAllServlet?ajaxAction=roadAllCache"
    web = requests.get(url)
    data = json.loads(web.text)["formData"]

    # 篩選條件：name 含「台北」或「臺北」
    filtered = [
        item for item in data
        if "臺北" in item["name"]
    ]

    # 依 number 欄位由大到小排序
    filtered = sorted(filtered, key=lambda x: x["number"], reverse=True)
        
    df = pd.DataFrame(filtered)[['srcdetail', 'happendate', 'roadtype', 'number', 'level', 'happentime', 'lastmodified', 'name', 'comment']]
    df['happentime'] = df['happendate'] + ' ' + df['happentime']
    df = df.drop(columns=["happendate"], errors="ignore")

    json_dict = df.to_dict(orient="records")
    return json_dict


def is_in_taipei(lat, lon):
    """
    判斷經緯度 (lat, lon) 是否落在臺北市的粗略邊界內。
    邊界：北緯 ≤ 25.2128 ≈ 25°12′46″；南緯 ≥ 24.9617 ≈ 24°57′42″；
            東經 ≤ 121.6583 ≈ 121°39′30″；西經 ≥ 121.4528 ≈ 121°27′10″
    """
    min_lat = 24.9617   # 南緯
    max_lat = 25.2128   # 北緯
    min_lon = 121.4528  # 西經
    max_lon = 121.6583  # 東經

    return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

def opendata_news_data():
    url = "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=36384FA8-FACF-432E-BB5B-5F015E7BC1BE"
    web = requests.get(url)
    data = json.loads(web.text)

    taipei_data = []
    for item in data:
        region = item.get("region", "").strip().upper()
        area = item.get("areaNm", "")
        x = item.get("x1", "")
        y = item.get("y1", "")
        # 僅篩 region == 'N'
        if region != "N" or item.get("happendate") != datetime.now().strftime("%Y-%m-%d"):
            continue

        # 若座標有效，再進一步檢查是否位於台北範圍
        try:
            lon = float(x)
            lat = float(y)
        except (ValueError, TypeError):
            continue

        if is_in_taipei(lat, lon):
            taipei_data.append(item)
        
    df = pd.DataFrame(taipei_data)
    json_dict = df.to_dict(orient="records")
    return json_dict
