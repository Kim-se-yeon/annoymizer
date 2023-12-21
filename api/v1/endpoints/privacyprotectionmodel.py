from fastapi import HTTPException, status
from fastapi import APIRouter
from schema.privacyprotectionmodel import PrivacyProtectionModelData
from .description import *
from . import send_request
import textwrap
import json

router = APIRouter()

session_endpoint = "http://172.16.124.111:8080/session"

### 프라이버시 보호 모델 ###
# K-익명성
# L-다양성
# T-근접성
# 현재 엔파이 구성은 아래의 조합 처럼 총 3가지 인데, 전부다 적용 가능할 것으로 보여짐
# 다만 모두 적용했을 경우에는 그만큼 가명화의 기준이 까다로워 지는 형태임
@router.post("/privacyprotectionmodel")
async def privacyprotectionmodel(data: PrivacyProtectionModelData) -> dict:
    session_id = data.session_id
    sensitive_column_list = list(data.sensitive_column_list)
    klt_select_list = list(data.klt_select_list)
    k_column_name_list = data.k_column_name_list
    k_value = data.k_value
    l_column_name_list = data.l_column_name_list
    l_value = data.l_value
    t_column_name_list = data.t_column_name_list
    t_value = data.t_value

    try:
        code = textwrap.dedent(f"""
        if {klt_select_list} == ["K"]:
            df_list = KAnonymity(input_df).check_k_anonymity(column_name_list={k_column_name_list}, k={k_value})
            rs_enough_df = df_list[0]
            rs_not_enough_df = df_list[1]
            result = df2_to_json(rs_enough_df.limit(20), rs_not_enough_df.limit(20))
            print(result)
        elif {klt_select_list} == ["K", "L"]:
            df_list = KAnonymity(input_df).check_k_anonymity(column_name_list={k_column_name_list}, k={k_value})
            enough_df = df_list[0]
            not_enough_df = df_list[1]
            df_list2 = LDiversity(enough_df).compute(column_name_list={l_column_name_list}, sensitive_column_list={sensitive_column_list}, l={l_value})
            rs_enough_df = df_list2[0]
            not_enough_df2 = df_list2[1]
            rs_not_enough_df = not_enough_df.union(not_enough_df2)
            result = df2_to_json(rs_enough_df.limit(20), rs_not_enough_df.limit(20))
            print(result)
        elif {klt_select_list} == ["K", "T"]:
            df_list = KAnonymity(input_df).check_k_anonymity(column_name_list={k_column_name_list}, k={k_value})
            enough_df = df_list[0]
            not_enough_df = df_list[1]
            df_list2 = TClosenessChecker2(enough_df).t_closeness_module(column_name_list={t_column_name_list}, sensitive_column_list={sensitive_column_list}, t={t_value})
            rs_enough_df = df_list2[0]
            not_enough_df2 = df_list2[1]
            rs_not_enough_df = not_enough_df.union(not_enough_df2)
            result = df2_to_json(rs_enough_df.limit(20), rs_not_enough_df.limit(20))
            print(result)
        else:
            print("error : 불가능한 조합입니다.")
        """)
        response = send_request(f"{session_endpoint}/statecode",
                                data={"code": code, "kind": "pyspark", "session_id": session_id})
        response['resultData']['data'] = json.loads(response['resultData']['data'])
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return response