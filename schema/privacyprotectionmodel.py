from pydantic import Field
from typing import List
from .session_management import SessionIdData


class PrivacyProtectionModelData(SessionIdData):
    sensitive_column_list: List[str] = Field(title='민감정보 컬럼 이름 리스트')
    klt_select_list: List[str] = Field(title='klt 조건 선택 리스트 (선택 가능 조함 : [K], [K,L], [K,T])')
    k_column_name_list: List[str] = Field(title='K 컬럼 이름 리스트')
    k_value: int = Field(title='K 값')
    l_column_name_list: List[str] = Field(None, title='L 컬럼 이름 리스트')
    l_value: int = Field(None, title='L 값')
    t_column_name_list: List[str] = Field(None, title='T 컬럼 이름 리스트')
    t_value: float = Field(None, title='T 값')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "sensitive_column_list": "민감정보 컬럼 이름 리스트",
                "klt_select_list": "K",
                "k_column_name_list": "K 컬럼 이름 리스트",
                "k_value": 100,
                "l_column_name_list": "L 컬럼 이름 리스트",
                "l_value": 100,
                "t_column_name_list": "T 컬럼 이름 리스트",
                "t_value": 100
            }
        }