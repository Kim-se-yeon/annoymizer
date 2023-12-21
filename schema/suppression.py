from pydantic import Field
from typing import List
from .session_management import SessionIdData
from typing import Optional

class SuppressionGeneralData(SessionIdData):
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름"
            }
        }


class SuppressionPartialData(SessionIdData):
    index_list: List[int] = Field(title='선택한 인덱스의 번호 리스트')
    index_reverse: bool = Field(title='인덱스 순서 리버스')
    separator: Optional[str] = Field(title='구분 값')
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "index_list": [1, 2, 3],
                "separator": "-",
                "index_reverse": True,
            }
        }


class SuppressionMaskingData(SessionIdData):
    index_list: List[int] = Field(title='선택한 인덱스의 번호 리스트')
    index_reverse: bool = Field(title='인덱스 순서 리버스')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "index_list": [1, 2, 3],
                "index_reverse": True,
            }
        }
