from pydantic import Field
from .session_management import SessionIdData

class OnewayEnrcyptionData(SessionIdData):
    algorithm: str = Field(title='알고리즘 (SHA224, SHA256, SHA384, SHA512)')
    encoding: str = Field(title='인코딩 방법 (hex, base64)')
    noise: str = Field(title='잡음 값')
    prefix: bool = Field(title='잡음값 위치 (앞)')
    suffix: bool = Field(title='잡음값 위치 (뒤)')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "algorithm": "SHA256",
                "encoding": "hex",
                "noise": "123",
                "prefix": True,
                "suffix": False
            }
        }


class TwowayEncryptionData(SessionIdData):
    encoding: str = Field(title='인코딩 방법 (hex, base64)')
    key: str = Field(title='암호화 키 값')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "encoding": "hex",
                "key": "123"
            }
        }


class FormatPreservingEncryptorData(SessionIdData):
    data_type: str = Field(
        title='암호화 대상 컬럼의 데이터 타입 (uppercase_letters, lowercase_letters, letters, digits, alphanumeric, ascii)')
    max_value_size: int = Field(title='암호화문의 최대길이')
    key_size: int = Field(title='암/복호화에 사용할 key의 길이')

    class Config:
        json_schema_extra = {
            "example": {
                "session_id": 123,
                "column_name": "컬럼 이름",
                "data_type": "uppercase_letters",
                "max_value_size": 10,
                "key_size": 10
            }
        }
