from fastapi import APIRouter
from .session_management import router as session_management_router
from .suppression import router as suppression_router
from .aggregation import router as aggregation_router
from .generalization import router as generalization_router
from .encrption import router as encrption_router
from .randomization import router as randomization_router
from .privacyprotectionmodel import router as privacyprotectionmodel_router


router = APIRouter()

router.include_router(session_management_router, prefix="/session", tags=["세션 관리"])
router.include_router(suppression_router, tags=["삭제 도구"])
router.include_router(aggregation_router,tags=["통계 도구"])
router.include_router(generalization_router, tags=["범주화 도구"])
router.include_router(encrption_router, tags=["암호화 도구"])
router.include_router(randomization_router, tags=["무작위화 도구"])
router.include_router(privacyprotectionmodel_router, tags=["프라이버시 보호 모델"])


@router.get("/health" , tags=["API 상태 확인"])
def health_check():
    return 'great health'