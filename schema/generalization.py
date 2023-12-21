from pydantic import Field
from typing import List, Tuple
from .session_management import SessionIdData

class RoundData(SessionIdData):
    digit: int = Field(title='자리수')
    round_type: str = Field(title='라운드 타입 ("nearest", "up", "down")')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "digit": 2,
                "round_type": "nearest"
            }
        }


class RandomRoundData(SessionIdData):
    digit: int = Field(title='자리수')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "digit": 2
            }
        }


class TopbottomCodingnData(SessionIdData):
    ratio: float = Field(title='비율')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "ratio": 0.5
            }
        }


class RangemethodStandardData(SessionIdData):
    min_value: int = Field(title='최소값')
    max_value: int = Field(title='최대값')
    standard_interval: int = Field(title='간격')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "min_value": 100,
                "max_value": 200,
                "standard_interval": 100
            }
        }


class RangemethodSettingData(SessionIdData):
    setting_interval: List[Tuple[int, int, str]] = Field(
        title='설정 간격 ex. [[0, 20, "Young"], [20, 30, "Adult"], [30, 50, "Middle-aged"], [50, 80, "Senior"]]')
    except_value: str = Field(None, title='예외 항목 값')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "digit": 2,
                "setting_interval": [[0, 20, "Young"], [20, 30, "Adult"], [30, 50, "Middle-aged"], [50, 80, "Senior"]],
                "except_value": "예외처리"
            }
        }


class CharcategorizationData(SessionIdData):
    char_tuples: List[Tuple[str, List[str]]] = Field(
        title='변환 문자 리스트 ex. [["경기", ["경기도 오산시 누읍동", "경기도 부천시 여월동"]], ["강원", ["강원도 춘천시 버들1길 103", "강원도춘천시 버들1길 103"]]]')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "char_tuples": [["경기", ["경기도 오산시 누읍동", "경기도 부천시 여월동"]], ["강원", ["강원도 춘천시 버들1길 103", "강원도춘천시 버들1길 103"]]]
            }
        }
