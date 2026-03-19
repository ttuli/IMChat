import os
import yaml
import nacos
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Type, TypeVar, Optional
from pydantic import BaseModel
from .config_loader import expand_env_recursive

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

class ConfigParser(ABC):
    @abstractmethod
    def load(self, target_config_class: Type[T]) -> T:
        pass

class FileParser(ConfigParser):
    def __init__(self, path: str):
        self.path = path

    def load(self, target_config_class: Type[T]) -> T:
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"Configuration file not found: {self.path}")
        
        with open(self.path, 'r', encoding='utf-8') as f:
            raw_data = yaml.safe_load(f)
            
        # Optional: Expands environment variables in the business configuration too
        expanded_data = expand_env_recursive(raw_data)
        
        return target_config_class(**expanded_data)

class NacosParser(ConfigParser):
    def __init__(self, nacos_config: Any):
        self.nacos_config = nacos_config
        self.client = nacos.NacosClient(
            self.nacos_config.host,
            namespace=self.nacos_config.namespace,
            username=self.nacos_config.user,
            password=self.nacos_config.password,
            timeout=self.nacos_config.timeout_ms / 1000.0 if self.nacos_config.timeout_ms else 5.0
        )

    def load(self, target_config_class: Type[T]) -> T:
        try:
            content = self.client.get_config(
                self.nacos_config.dataid, 
                self.nacos_config.group
            )
            
            if not content:
                raise ValueError(f"No configuration found in Nacos for dataid: {self.nacos_config.dataid}, group: {self.nacos_config.group}")
            
            # Assume Nacos content is YAML
            raw_data = yaml.safe_load(content)
            
            # Expand environment variables in the configuration content
            expanded_data = expand_env_recursive(raw_data)
            
            return target_config_class(**expanded_data)
        except Exception as e:
            logger.error(f"Failed to load configuration from Nacos: {e}")
            raise
