from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/news", tags=["警廣 News"])

@router.get("/police", response_class=JSONResponse)
def get_news():
    from functions.police import police_news_data
    data = police_news_data()
    return data

@router.get("/police_local", response_class=JSONResponse)
def get_news_local():
    import requests
    url = "https://hack.acthub.pro/police-news"
    response = requests.get(url)
    data = response.json()
    return data

@router.get("/police_opendata", response_class=JSONResponse)
def get_opendata_news():
    from functions.police import opendata_news_data
    data = opendata_news_data()
    return data
