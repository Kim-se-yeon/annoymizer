from fastapi import HTTPException, status
from fastapi import APIRouter
from schema.randomization import NoiseAdditionData, PermutationData, PermutationGroupnData, RandomNumberGeneratorData
from .description import *
from . import send_request
import json

router = APIRouter()

session_endpoint = "http://172.16.124.111:8080/session"

### 무작위화 ###
# 잡음 추가
@router.post("/randomization/noise-addition", responses=response_module_value)
async def randomization_noise_addition(data: NoiseAdditionData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    calc = data.calc
    val = data.val

    try:
        code = f"""print(Randomization(input_df).noise_addition(column_name='{column_name}', calc='{calc}', val={val}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 순열
@router.post("/randomization/permutation", responses=response_module_value)
async def randomization_permutation(data: PermutationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Randomization(input_df).permutation(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 그룹별 순열
@router.post("/randomization/permutation-group", responses=response_module_value)
async def randomization_permutation_group(data: PermutationGroupnData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    group_column = data.group_column

    try:
        code = f"""print(Randomization(input_df).permutation_group(column_name='{column_name}', group_column='{group_column}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 난수 생성기
@router.post("/randomization/random-number-generator", responses=response_module_value)
async def randomization_random_number_generator(data: RandomNumberGeneratorData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Randomization(input_df).random_number_generator(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response