from pydantic import Field
from .session_management import SessionIdData

class AggregationData(SessionIdData):
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름"
            }
        }


class MicroAggregationData(SessionIdData):
    start_value: int = Field(title='시작 값')
    end_value: int = Field(title='종료 값')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "start_value": 100,
                "end_value": 200
            }
        }