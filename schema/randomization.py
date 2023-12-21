from pydantic import Field
from .session_management import SessionIdData

class NoiseAdditionData(SessionIdData):
    calc: str = Field(title='계산기호 ("+", "-", "*", "/", "%")')
    val: int = Field(title='잡음처리 추가 값')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "calc": "+",
                "val": 100
            }
        }


class PermutationData(SessionIdData):
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름"
            }
        }


class PermutationGroupnData(SessionIdData):
    group_column: str = Field(title='그룹 시킬 컬럼 이름')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "group_column": "컬럼 이름"
            }
        }


class RandomNumberGeneratorData(SessionIdData):
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름"
            }
        }
