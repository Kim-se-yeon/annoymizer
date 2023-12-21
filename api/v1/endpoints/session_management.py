from fastapi import HTTPException, status
from fastapi import APIRouter
from schema.session_management import SessionManageData, SessionData
from . import send_request
from .description import *
import textwrap
import requests
import time

router = APIRouter()

session_endpoint = "http://172.16.124.111:8080/session"

def wait_for_idle_state(session_id):
    session_url = f"{session_endpoint}/state?session_id={session_id}"
    session_count = 0
    while True:
        try:
            response = send_request(session_url, method="GET")
            state = response.get("state", "starting")
            if state == "idle":
                break
            elif state == "dead":
                send_request(f"{session_endpoint}/finish-session", data={"session_id": session_id})
                raise ValueError('Session dead')
            time.sleep(1)
            session_count += 1
            if session_count >= 60:
                raise ValueError('Session create error occurred')
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Error occurred while waiting for session to become idle: {str(e)}")


@router.post("/create", responses=session_create_value)
async def session_create() -> dict:
    url = f"{session_endpoint}/create"
    payload = {
        "driverMemory": "1G",
        "driverCores": 1,
        "executorMemory": "1G",
        "executorCores": 1,
        "numExecutors": 1
    }
    try:
        response = send_request(url, data=payload)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    session_id = response.get('session_id')
    wait_for_idle_state(session_id)

    if response.get("resultCode") == 200:

        code = textwrap.dedent(f"""
                from Suppression import Suppression
                from Aggregation import Aggregation, MicroAggregation
                from Generalization import Rounding, TopBottomCoding, CharCategorization, RangeMethod
                from Encryption import OneWayEncryption, TwoWayEncryption, FormatPreservingEncryptor
                from Randomization import Randomization
                from PrivacyProtectionModel import KAnonymity, LDiversity, TClosenessChecker, TClosenessChecker2
                from dataframe_to_json import df2_to_json
        """)
        try:
            response2 = send_request(f"{session_endpoint}/statecode",
                                     data={"code": code, "kind": "pyspark", "session_id": session_id})
        except HTTPException as e:
            raise HTTPException(status_code=e.status_code, detail=str(e.detail))
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        if response2.get("resultCode") == 200:
            return response


@router.post("/delete/{session_id}", responses=session_delete_value)
async def session_delete(session_id: int) -> dict:
    url = f"{session_endpoint}/finish-session"
    payload = {
        "session_id": session_id,
    }
    try:
        response = send_request(url, data=payload)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    return response


@router.get("/state-check/{session_id}", responses=session_state_value)
async def session_state_check(session_id: int) -> dict:
    url = f"{session_endpoint}/state?session_id={session_id}"
    try:
        response = send_request(url, method="GET")
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    return response


@router.get("/searchstcode/{session_id}/{statement_id}", responses=session_searchstcode_value)
async def session_statement_code(session_id: int, statement_id: int) -> dict:
    url = f"{session_endpoint}/searchstcode?session_id={session_id}&statement_id={statement_id}"
    try:
        response = send_request(url, method="GET")
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    return response

@router.post("/import")
async def library_import(data: SessionManageData) -> dict:
    session_id = data.session_id
    code = textwrap.dedent(f"""
                    from Suppression import Suppression
                    from Aggregation import Aggregation, MicroAggregation
                    from Generalization import Rounding, TopBottomCoding, CharCategorization, RangeMethod
                    from Encryption import OneWayEncryption, TwoWayEncryption, FormatPreservingEncryptor
                    from Randomization import Randomization
                    from PrivacyProtectionModel import KAnonymity, LDiversity, TClosenessChecker, TClosenessChecker2
                    from dataframe_to_json import df2_to_json
            """)
    try:
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    return response


@router.post("/data-load")
async def data_load(data: SessionManageData) -> dict:
    session_id = data.session_id
    try:
        code = """input_df = spark.read.csv("file:///usr/local/spark/anonymizer/data/npie_test.csv", header=True, inferSchema=True)"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    return response