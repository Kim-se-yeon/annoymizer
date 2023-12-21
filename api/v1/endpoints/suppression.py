from fastapi import HTTPException, status
from fastapi import APIRouter
from schema.suppression import SuppressionGeneralData, SuppressionPartialData, SuppressionMaskingData
from .description import *
from . import send_request
import json

router = APIRouter()

session_endpoint = "http://172.16.124.111:8080/session"

### 삭제 ###
# 삭제
@router.post("/suppression/general", responses=response_module_value)
async def suppression_general(data: SuppressionGeneralData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Suppression(input_df).general(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 부분삭제
@router.post("/suppression/partial", responses=response_module_value)
async def suppression_partial(data: SuppressionPartialData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    index_list = data.index_list
    separator = data.separator
    index_reverse = data.index_reverse

    try:
        code = f"""print(Suppression(input_df).partial(column_name='{column_name}', index_list={index_list}, separator="{separator}", index_reverse={index_reverse}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode", data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 마스킹 삭제
@router.post("/suppression/masking", responses=response_module_value)
async def suppression_masking(data: SuppressionMaskingData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    index_list = data.index_list
    index_reverse = data.index_reverse

    try:
        code = f"""print(Suppression(input_df).masking(column_name='{column_name}', index_list={index_list}, index_reverse={index_reverse}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response