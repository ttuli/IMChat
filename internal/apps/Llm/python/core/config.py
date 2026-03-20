import os
from pydantic import BaseModel, Field, model_validator
from .config_loader import ConfigLoader


class RedisConf(BaseModel):
    host: str = Field("", alias="Host")
    redis_type: str = Field("redis", alias="Type")
    password: str = Field("", alias="Pass")


class JwtConf(BaseModel):
    secret: str = Field("", alias="Secret")
    expire: int = Field(0, alias="Expire")
    refresh_expire: int = Field(0, alias="RefreshExpire")


class TokenConf(BaseModel):
    redis_conf: RedisConf = Field(default_factory=RedisConf, alias="RedisConf")
    jwt_config: JwtConf = Field(default_factory=JwtConf, alias="JWTConfig")


class SuggestLlmConf(BaseModel):
    api_key: str = Field(..., alias="SuggestLlmApiKey")
    url: str = Field(..., alias="SuggestLlmUrl")
    model: str = Field(..., alias="SuggestLlmModel")
    prompt: str = Field("", alias="SuggestLlmPrompt")


class ApisixConf(BaseModel):
    enable: bool = Field(False, alias="Enable")
    url: str = Field("http://127.0.0.1:9180", alias="AdminURL")
    admin_key: str = Field("your-api-key", alias="APIKey")
    upstream_id: str = Field("llm-suggest-upstream", alias="UpstreamID")
    service_ip: str = Field("", alias="ServiceIP")
    route_url: str = Field("/ai", alias="RouteURI")
    route_id: str = Field("llm-suggest-route", alias="RouteID")


class AppConfig(BaseModel):
    # API configuration
    suggest_llm_config: SuggestLlmConf = Field(..., alias="SuggestLlmConfig")
    
    # Server configuration
    port: int = Field(8000, alias="Port")
    host: str = Field("0.0.0.0", alias="Host")
    
    # APISIX configuration
    apisix_config: ApisixConf = Field(default_factory=ApisixConf, alias="ApisixConfig")

    # Token configuration
    token_config: TokenConf = Field(..., alias="TokenConfig")

    def get_jwt_secret(self) -> str:
        return self.token_config.jwt_config.secret

    def get_redis_host(self) -> str:
        return self.token_config.redis_conf.host

    def get_redis_password(self) -> str:
        return self.token_config.redis_conf.password

def load_config() -> AppConfig:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "..", "etc", "config.yaml")
    loader = ConfigLoader(config_path)
    return loader.load(AppConfig)


try:
    config = load_config()
except Exception as e:
    print(f"Error loading configuration: {e}")
    raise e