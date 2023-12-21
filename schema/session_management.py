from pydantic import BaseModel, Field


class SessionManageData(BaseModel):
    session_id: str = Field(title='세션 id')


class SessionIdData(BaseModel):
    session_id: str = Field(title='세션 id')
    column_name: str = Field(title='컬럼 이름')


class SessionData(SessionIdData):
    user_id: str = Field(title='사용자 고유 id')
    company_id: str = Field(title='회사 고유 id')
    user_name: str = Field(title='이름')
    manager_id: str = Field(title='담당자 고유 id')
    session_name: str = Field(title='세션 이름')
    kind: str = Field(title='사양 종류 ("normal", "memeber", "club", "pro")')
    start_timer: int = Field(title='세션 유지 시간')

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": '1234',
                "company_id": '123456789',
                "user_name": "김더존",
                "manager_id": "4321",
                "session_name": "세션 이름",
                "kind": "normal",
                "start_timer": 123
            }
        }