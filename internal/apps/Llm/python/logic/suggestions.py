import os
import json
from volcenginesdkarkruntime import Ark
from ..core.config import config

class DoubaoLlmService:
    def __init__(self):
        self.api_key = config.suggest_llm_api_key
        self.base_url = config.suggest_llm_url
        self.model = config.suggest_llm_model
        # Go implementation had a Prompt field in LlmProviderConfig
        self.prompt = config.suggest_llm_prompt
        
        if not self.api_key:
            raise ValueError("SuggestLlmApiKey not found in environment variables")

        self.client = Ark(
            api_key=self.api_key,
            base_url=self.base_url
        )

    def suggestions(self, history: list) -> list:
        """
        history: list of dicts with 'role' (int) and 'content' (str)
        role: 0 (assistant), 1 (user), 2 (system)
        """
        chat_messages = []
        if self.prompt:
            chat_messages.append({"role": "system", "content": self.prompt})
        
        for msg in history:
            # Following Go logic in doubao.go:
            # Role_ROLE_ME (0) -> assistant
            # Role_ROLE_OTHER (1) & Role_ROLE_SYSTEM (2) -> user
            role = "assistant" if msg.get("role") == 0 else "user"
            chat_messages.append({
                "role": role,
                "content": msg.get("content", "")
            })
            
        completion = self.client.chat.completions.create(
            model=self.model,
            messages=chat_messages,
        )
        
        if not completion.choices:
            return []
            
        content = completion.choices[0].message.content
        try:
            # Replicate the JSON parsing from Go: result.Suggestions
            # Expected format: {"suggestions": ["opt1", "opt2", ...]}
            result = json.loads(content)
            return result.get("suggestions", [])
        except json.JSONDecodeError as e:
            # Log or raise as in Go's fmt.Errorf
            print(f"Failed to parse LLM response: {content}, Error: {e}")
            return []
