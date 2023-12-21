from fastapi import HTTPException, status
from fastapi import APIRouter
from schema.generalization import RoundData, RandomRoundData, TopbottomCodingnData, RangemethodStandardData, RangemethodSettingData, CharcategorizationData
from .description import *
from . import send_request
import json

router = APIRouter()

session_endpoint = "http://172.16.124.111:8080/session"

### 일반화 ###
# 일반 라운딩
@router.post("/rounding/round", responses=response_module_value)
async def rounding_round(data: RoundData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    digit = data.digit
    round_type = data.round_type

    try:
        code = f"""print(Rounding(input_df).round(column_name='{column_name}', digit={digit}, round_type="{round_type}", preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 랜덤 라운딩
@router.post("/rounding/random-round", responses=response_module_value)
async def rounding_random_round(data: RandomRoundData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    digit = data.digit

    try:
        code = f"""print(Rounding(input_df).random_round(column_name='{column_name}', digit={digit}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 상하단 코딩
@router.post("/topbottomcoding", responses=response_module_value)
async def topbottom_coding(data: TopbottomCodingnData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    ratio = data.ratio

    try:
        code = f"""print(TopBottomCoding(input_df).topbottom_coding(column_name='{column_name}', ratio={ratio}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 범위 방법(일정 간격)
@router.post("/rangemethod/standard-interval", responses=response_module_value)
async def range_method_standard_interval(data: RangemethodStandardData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    min_value = data.min_value
    max_value = data.max_value
    standard_interval = data.standard_interval

    try:
        code = f"""print(RangeMethod(input_df).range_method_standard_interval(column_name='{column_name}', min_value={min_value}, max_value={max_value}, standard_interval={standard_interval}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 범위 방법(임의의 수 기준)
@router.post("/rangemethod/setting-interval", responses=response_module_value)
async def range_method_setting_interval(data: RangemethodSettingData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    setting_interval = data.setting_interval
    except_value = data.except_value

    try:
        code = f"""print(RangeMethod(input_df).range_method_setting_interval(column_name='{column_name}', setting_interval={setting_interval}, except_value={except_value},preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 문자데이터 범주화
@router.post("/charcategorization", responses=response_module_value)
async def charcategorization_replace_chars_column(data: CharcategorizationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    char_tuples = data.char_tuples

    try:
        code = f"""print(CharCategorization(input_df).replace_chars_column(column_name='{column_name}', char_tuples={char_tuples}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response