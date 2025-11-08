from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from util.nowtime import getTaiwanTime

router = APIRouter(prefix="/issue", tags=["障礙回報"])

@router.post("/create", response_class=JSONResponse)
def create_issue(address: str, obstacle_type: str, description: str, modtified_userid: str = "visitor"):
    from functions.report import insert_issue
    insert_issue(
        address=address,
        obstacle_type=obstacle_type,
        description=description,
        time=getTaiwanTime(),
        modtified_userid=modtified_userid
    )
    return '回報成功'

@router.get("/getByTime", response_class=JSONResponse)
def get_issues_by_time(hours: int = 24):
    from functions.report import read_issues_by_time
    issues = read_issues_by_time(hours=hours)
    return issues

@router.get("/getByStatus", response_class=JSONResponse)
def get_issues_by_status(status: str = "Unsolved"):
    from functions.report import read_issues_by_status
    issues = read_issues_by_status(status=status)
    return issues

@router.post("/update", response_class=JSONResponse)
def update_issue(issue_id: str, new_status: str = "Solved", modtified_userid: str = "visitor"):
    from functions.report import update_issue_status
    result = update_issue_status(issue_id=issue_id, new_status=new_status, modtified_userid=modtified_userid)
    return result