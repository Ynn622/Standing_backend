from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/wind", tags=["風速資料"])

@router.get("/", response_class=JSONResponse)
def get_wind_speed():
    from functions.windspeed import windspeed_taipei
    data = windspeed_taipei()
    return data

@router.get("/future", response_class=JSONResponse)
def get_future_wind_speed():
    from functions.weather_future import windspeed_taipei_future
    data = windspeed_taipei_future()
    return data