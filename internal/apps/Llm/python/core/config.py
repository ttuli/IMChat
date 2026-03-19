import os
from pydantic import BaseModel, Field
from .config_loader import ConfigLoader

class AppConfig(BaseModel):
    # API configuration
    suggest_llm_api_key: str = Field(..., alias="SuggestLlmApiKey")
    suggest_llm_url: str = Field(..., alias="SuggestLlmUrl")
    suggest_llm_model: str = Field(..., alias="SuggestLlmModel")
    suggest_llm_prompt: str = Field("", alias="SuggestLlmPrompt")
    
    # Server configuration
    port: int = Field(8000, alias="port")
    host: str = Field("0.0.0.0", alias="host")
    
    # APISIX configuration
    apisix_url: str = Field("http://127.0.0.1:9180", alias="APISIX_HOST")
    apisix_admin_key: str = Field("your-api-key", alias="APISIX_ADMIN_KEY")

def load_config() -> AppConfig:
    # Get the directory of the current file (core/)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Adjust path to find etc/config.yaml relative to internal/apps/Llm/python/
    config_path = os.path.join(current_dir, "..", "etc", "config.yaml")
    
    loader = ConfigLoader(config_path)
    return loader.load(AppConfig)

# Initialize global config
try:
    config = load_config()
except Exception as e:
    # Fallback or exit depending on requirements. 
    # For now, we'll let it raise so the service doesn't start with invalid config.
    print(f"Error loading configuration: {e}")
    raise e