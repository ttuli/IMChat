from fastapi import APIRouter
from ..types.suggestions import SuggestRequest, SuggestResponse
from ..logic.suggestions import DoubaoLlmService

router = APIRouter()
llm_service = DoubaoLlmService()

@router.post("/suggestions", response_model=SuggestResponse)
def get_suggestions(req: SuggestRequest):
    # 将 Pydantic 对象转换为字典列表传给 service
    history = [m.model_dump() for m in req.history]
    reply = llm_service.suggestions(history)
    return SuggestResponse(reply=reply)
