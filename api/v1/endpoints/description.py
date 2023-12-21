response_module_value = {
    200: {
        "description": "가명처리 결과 프리뷰 dictionary 형태",
        "content": {
            "application/json": {
                "example": {
                    "resultData": {
                        "data": {
                            "before": {
                                "컬럼이름": []
                            },
                            "after": {
                                "컬럼이름_new": []
                            }
                        }
                    },
                    "resultCode": 200,
                    "session_id": 627,
                    "statement_id": 2,
                    "execution_state": "ok",
                    "resultMsg": "statementcode 추가 완료"
                }
            }
        },
    }
}

session_create_value = {
    200: {
        "description": "session 생성 후 반환 값",
        "content": {
            "application/json": {
                "example": {
                    "resultCode": 200,
                    "session_Id": 581,
                    "state": "starting",
                    "resultMsg": "세션 생성 완료."
                }
            }
        },
    }
}

session_delete_value = {
    200: {
        "description": "session 삭제 후 반환 값",
        "content": {
            "application/json": {
                "example": {
                    "resultCode": 200,
                    "resultData": {
                        "delete_session": 581
                    },
                    "resultMsg": "세션 삭제 완료"
                }
            }
        },
    }
}

session_state_value = {
    200: {
        "description": "session 상태 반환 값",
        "content": {
            "application/json": {
                "example": {
                    "resultCode": 200,
                    "session_id": 581,
                    "state": "idle",
                    "resultMsg": "세션 상태 조회 완료"
                }
            }
        },
    }
}

session_searchstcode_value = {
    200: {
        "description": "session statement_id 결과 값",
        "content": {
            "application/json": {
                "example": {
                    "resultData": {
                        "data": ""
                    },
                    "resultCode": 200,
                    "session_id": 581,
                    "statement_id": 0,
                    "resultMsg": "statement 데이터 조회 성공"
                }
            }
        },
    }
}
