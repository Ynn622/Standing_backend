from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

def getTaiwanTime(ms: bool = False) -> str:
    """
    取得台灣目前時間。
    Args:
        ms (bool): 是否包含毫秒 (預設 False)。
    Returns: 
        str: "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD HH:MM:SS:SSS"
    """
    taiwan_now = datetime.now(ZoneInfo("Asia/Taipei"))
    base_time = taiwan_now.strftime("%Y-%m-%d %H:%M:%S")

    if ms:
        # YYYY-MM-DD HH:MM:SS:SSS
        return base_time + f":{taiwan_now.microsecond // 1000:03d}"
    else:
        # YYYY-MM-DD HH:MM:SS
        return base_time
    
def getFutureTime() -> dict:
    """
    取得台灣目前小時的起點時間與明天同小時時間。
    Returns: 
        dict: {
            "timeFrom": "YYYY-MM-DDTHH:00:00",
            "timeTo": "YYYY-MM-DDTHH:00:00"
        }
    """
    taiwan_now = datetime.now(ZoneInfo("Asia/Taipei"))
    start = taiwan_now.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    return {
        "timeFrom": start.strftime("%Y-%m-%dT%H:%M:01"),
        "timeTo": end.strftime("%Y-%m-%dT%H:%M:01")
    }