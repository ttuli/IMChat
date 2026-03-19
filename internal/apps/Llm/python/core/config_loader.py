import os
import re
import yaml
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

def expand_env(s: str) -> str:
    """
    Expands environment variables in the format ${VAR} or $VAR.
    Matches Go's os.ExpandEnv behavior.
    """
    if not isinstance(s, str):
        return s
    
    # Pattern for ${VAR} or $VAR
    pattern = re.compile(r'\$(\{?)([a-zA-Z_][a-zA-Z0-9_]*)(\}?)')
    
    def replace(match):
        prefix = match.group(1)
        name = match.group(2)
        suffix = match.group(3)
        
        # Ensure symmetric braces if present
        if (prefix == '{' and suffix == '}') or (prefix == '' and suffix == ''):
            return os.getenv(name, match.group(0))
        return match.group(0)

    return pattern.sub(replace, s)

def expand_env_recursive(data: Any) -> Any:
    """
    Recursively expands environment variables in strings within dicts or lists.
    """
    if isinstance(data, dict):
        return {k: expand_env_recursive(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [expand_env_recursive(i) for i in data]
    elif isinstance(data, str):
        return expand_env(data)
    else:
        return data

class NacosConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8848
    namespace: str = ""
    user: str = "nacos"
    password: str = "nacos"
    dataid: str = ""
    group: str = "DEFAULT_GROUP"
    timeout_ms: int = Field(5000, alias="timeoutms")
    not_load_cache_at_start: bool = Field(False, alias="notloadcacheatstart")

class FileConfig(BaseModel):
    path: str = ""

class LocalConfig(BaseModel):
    config_source: str = Field(..., alias="config_source")
    nacos: Optional[NacosConfig] = None
    file: Optional[FileConfig] = None

class ConfigLoader:
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.local_config: Optional[LocalConfig] = None

    def load_local_config(self) -> LocalConfig:
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        
        with open(self.config_file, 'r', encoding='utf-8') as f:
            raw_data = yaml.safe_load(f)
        
        # Expand environment variables
        expanded_data = expand_env_recursive(raw_data)
        
        self.local_config = LocalConfig(**expanded_data)
        return self.local_config

    def get_parser(self):
        from .parsers import FileParser, NacosParser
        
        if not self.local_config:
            self.load_local_config()
            
        source = self.local_config.config_source
        if source == "file":
            if not self.local_config.file.path:
                raise ValueError("File configuration path is missing")
            return FileParser(self.local_config.file.path)
        elif source == "nacos":
            if not self.local_config.nacos:
                raise ValueError("Nacos configuration is missing")
            return NacosParser(self.local_config.nacos)
        else:
            raise ValueError(f"Unsupported config source: {source}")

    def load(self, target_config_class):
        parser = self.get_parser()
        return parser.load(target_config_class)
