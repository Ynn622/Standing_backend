import requests
import json
import pandas as pd

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
