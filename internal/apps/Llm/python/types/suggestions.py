from typing import List
from pydantic import BaseModel

class ChatMessage(BaseModel):
    role: int
    content: str

class SuggestRequest(BaseModel):
    history: List[ChatMessage]

class SuggestResponse(BaseModel):
    reply: List[str]
