from supabase import create_client, Client
from datetime import datetime, timedelta

from util.config import env
from util.nowtime import getTaiwanTime

# 初始化 Supabase 連線
supabase: Client = create_client(env.SUPABASE_URL, env.SUPABASE_KEY)

def insert_issue(address: str, obstacle_type: str, description: str, time: datetime):
    """
    將一筆障礙回報資料寫入 Supabase 資料庫。
    """
    data = {
        "address": address,
        "type": obstacle_type,
        "description": description,
        "time": time.isoformat() if isinstance(time, datetime) else time
    }

    try:
        response = supabase.table("issues").insert(data).execute()
        if response.data:
            return {'success': True, 'data': response.data}
        else:
            print("⚠️ 資料寫入失敗:", response)
            return {'success': False, 'data': None}
    except Exception as e:
        print("❌ 發生錯誤:", e)
        return {'success': False, 'data': f'error: {str(e)}'}

def read_issues_by_time(hours: int = 24):
    """
    取得最近 N 小時內的障礙通報資料，預設 24 小時。
    """
    try:
        now = getTaiwanTime()
        start_time = now - timedelta(hours=hours)

        response = (
            supabase
            .table("issues")
            .select("*")
            .gte("time", start_time.isoformat())  # 過濾時間欄位
            .order("time", desc=True)
            .execute()
        )
        
        data = response.data or []

        # 🔧 時間格式轉換
        for item in data:
            if "time" in item and item["time"]:
                # 解析 ISO 格式的時間字串，去掉 T 和 Z
                try:
                    dt = datetime.fromisoformat(item["time"].replace("Z", ""))
                    item["time"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass  # 若格式異常，保留原始字串

        return data
    except Exception as e:
        print("❌ 查詢錯誤:", e)
        return None

def read_issues_by_status(status: str | None = None):
    """
    根據狀態字串查詢障礙通報資料。
    例如：
        read_issues_by_status("unsolved") → status='unsolved'
        read_issues_by_status("solved")   → status='solved'
        read_issues_by_status()           → 不篩選 status，全取
    """
    try:
        query = supabase.table("issues").select("*")

        # 👇 若有指定 status，則加上條件
        if status:
            query = query.eq("status", status)

        response = query.order("time", desc=True).execute()
        data = response.data or []

        # 🔧 格式化時間
        for item in data:
            if "time" in item and item["time"]:
                try:
                    dt = datetime.fromisoformat(item["time"].replace("Z", ""))
                    item["time"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass

        return data

    except Exception as e:
        print("❌ 查詢錯誤:", e)
        return None

def update_issue_status(issue_id: str, new_status: str = "solved"):
    """
    根據指定 id，將 issues 資料表中的 status 更新為指定狀態。
    預設 new_status='solved'。
    """
    try:
        response = (
            supabase
            .table("issues")
            .update({"status": new_status})
            .eq("id", issue_id)
            .execute()
        )

        if response.data:
            print(f"✅ 已更新 id={issue_id} 的狀態為 '{new_status}'")
            return response.data
        else:
            print(f"⚠️ 找不到 id={issue_id} 的資料")
            return None

    except Exception as e:
        print("❌ 更新錯誤:", e)
        return None