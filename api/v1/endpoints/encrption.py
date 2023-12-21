from fastapi import HTTPException, status
from fastapi import APIRouter
from schema.encrption import OnewayEnrcyptionData, TwowayEncryptionData, FormatPreservingEncryptorData
from .description import *
from . import send_request
import json

router = APIRouter()

session_endpoint = "http://172.16.124.111:8080/session"

### 암호화 ###
# 일방향 임호화
@router.post("/onewayencryption/apply-hash", responses=response_module_value)
async def onewayencryption_apply_hash(data: OnewayEnrcyptionData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    algorithm = data.algorithm
    encoding = data.encoding
    noise = data.noise
    prefix = data.prefix
    suffix = data.suffix

    try:
        code = f"""print(OneWayEncryption(input_df).apply_hash(column_name='{column_name}', algorithm='{algorithm}', encoding='{encoding}', noise='{noise}', prefix={prefix}, suffix={suffix}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 일방향 임호화
@router.post("/twowayencryption/apply-encrypt", responses=response_module_value)
async def twowayencryption_apply_encrypt(data: TwowayEncryptionData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    key = data.key
    encoding = data.encoding

    try:
        code = f"""print(TwoWayEncryption(input_df).apply_encrypt(column_name='{column_name}', key='{key}', encoding='{encoding}', preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response


# 형태보존 임호화
@router.post("/formatpreservingencryptor/apply-encrypt", responses=response_module_value)
async def formatpreservingencryptor_apply_encrypt(data: FormatPreservingEncryptorData) -> dict:
    session_id = data.session_id
    column_name = data.column_name
    data_type = data.data_type
    max_value_size = data.max_value_size
    key_size = data.key_size

    try:
        code = f"""print(FormatPreservingEncryptor(input_df).apply_encrypt(column_name='{column_name}', data_type='{data_type}', max_value_size={max_value_size}, key_size={key_size}, preview='T'))"""
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response