from fastapi import HTTPException, status
from fastapi import APIRouter
from schema.aggregation import AggregationData, MicroAggregationData
from .description import *
from . import send_request
import json

router = APIRouter()

session_endpoint = "http://172.16.124.111:8080/session"

### 총계처리 ###
@router.post("/aggregation/sum", responses=response_module_value)
async def aggregation_sum(data: AggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Aggregation(input_df).sum(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/aggregation/avg", responses=response_module_value)
async def aggregation_avg(data: AggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Aggregation(input_df).avg(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/aggregation/max", responses=response_module_value)
async def aggregation_max(data: AggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Aggregation(input_df).max(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/aggregation/min", responses=response_module_value)
async def aggregation_min(data: AggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Aggregation(input_df).min(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/aggregation/mode", responses=response_module_value)
async def aggregation_mode(data: AggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Aggregation(input_df).mode(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/aggregation/median", responses=response_module_value)
async def aggregation_median(data: AggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name

    try:
        code = f"""print(Aggregation(input_df).median(column_name='{column_name}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/microaggregation/sum", responses=response_module_value)
async def microaggregation_sum(data: MicroAggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    start_value = data.start_value
    end_value = data.end_value

    try:
        code = f"""print(MicroAggregation(input_df).sum(column_name='{column_name}', start_value={start_value}, end_value={end_value}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/microaggregation/avg", responses=response_module_value)
async def microaggregation_avg(data: MicroAggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    start_value = data.start_value
    end_value = data.end_value

    try:
        code = f"""print(MicroAggregation(input_df).avg(column_name='{column_name}', start_value={start_value}, end_value={end_value}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/microaggregation/max", responses=response_module_value)
async def microaggregation_max(data: MicroAggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    start_value = data.start_value
    end_value = data.end_value

    try:
        code = f"""print(MicroAggregation(input_df).max(column_name='{column_name}', start_value={start_value}, end_value={end_value}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/microaggregation/min", responses=response_module_value)
async def microaggregation_min(data: MicroAggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    start_value = data.start_value
    end_value = data.end_value

    try:
        code = f"""print(MicroAggregation(input_df).min(column_name='{column_name}', start_value={start_value}, end_value={end_value}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/microaggregation/mode", responses=response_module_value)
async def microaggregation_mode(data: MicroAggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    start_value = data.start_value
    end_value = data.end_value

    try:
        code = f"""print(MicroAggregation(input_df).mode(column_name='{column_name}', start_value={start_value}, end_value={end_value}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


@router.post("/microaggregation/median", responses=response_module_value)
async def microaggregation_median(data: MicroAggregationData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    start_value = data.start_value
    end_value = data.end_value

    try:
        code = f"""print(MicroAggregation(input_df).median(column_name='{column_name}', start_value={start_value}, end_value={end_value}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response