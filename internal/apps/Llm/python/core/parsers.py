import os
import asyncio
import yaml
import logging
from abc import ABC, abstractmethod
from typing import Any, Type, TypeVar
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
    """
    注意：load() 方法只应调用一次。
    v2 模式下，load() 执行完毕后会关闭事件循环和客户端连接，之后不可再次调用。
    """

    def __init__(self, nacos_config: Any):
        self.nacos_config = nacos_config
        self._use_v2 = False
        self._loop = None
        self.client = self._build_client()

    def _build_client(self):
        # Prefer nacos-sdk-python v2 API, fallback to legacy nacos.NacosClient.
        try:
            from v2.nacos import ClientConfigBuilder, NacosConfigService
        except Exception as v2_error:
            # v2 SDK not available, fallback to legacy client
            logger.debug(f"v2 nacos SDK not available: {v2_error}")
            try:
                import nacos

                self._use_v2 = False
                return nacos.NacosClient(
                    self.nacos_config.host,
                    namespace=self.nacos_config.namespace,
                    username=self.nacos_config.user,
                    password=self.nacos_config.password,
                    timeout=self.nacos_config.timeout_ms / 1000.0 if self.nacos_config.timeout_ms else 5.0,
                )
            except Exception as legacy_error:
                raise RuntimeError(
                    "Failed to initialize Nacos client. "
                    "Install nacos-sdk-python and avoid local module name conflicts like 'nacos/'."
                ) from legacy_error

        # v2 SDK available
        self._use_v2 = True
        self._loop = asyncio.new_event_loop()

        try:
            server_address = f"{self.nacos_config.host}:{self.nacos_config.port}"
            builder = (
                ClientConfigBuilder()
                .server_address(server_address)
                .namespace_id(self.nacos_config.namespace)
                .username(self.nacos_config.user)
                .password(self.nacos_config.password)
            )
            if self.nacos_config.timeout_ms:
                builder = builder.timeout_ms(self.nacos_config.timeout_ms)

            client_config = builder.build()
            client = NacosConfigService.create_config_service(client_config)
            return self._run_coro(client)
        except Exception:
            # 初始化失败时及时关闭 loop，避免资源泄漏
            self._loop.close()
            self._loop = None
            raise

    def _run_coro(self, coro):
        if self._loop is None:
            raise RuntimeError("Nacos async event loop is not initialized")
        return self._loop.run_until_complete(coro)

    async def _shutdown_v2_client(self):
        """关闭 v2 客户端，并等待所有 gRPC 内部任务结束。"""
        try:
            if self.client is not None:
                await self.client.shutdown()
                # 给 gRPC 内部任务（如 _server_request_watcher）一点时间自行结束
                await asyncio.sleep(0.1)
        except Exception:
            logger.warning("Failed to shutdown Nacos v2 client cleanly", exc_info=True)

        # 排除当前任务自身，避免 gather 等待自己导致死锁
        current = asyncio.current_task()
        pending = {t for t in asyncio.all_tasks(self._loop) if t is not current}
        if pending:
            logger.debug(f"Waiting for {len(pending)} pending nacos task(s) to finish...")
            await asyncio.gather(*pending, return_exceptions=True)

    def load(self, target_config_class: Type[T]) -> T:
        try:
            if self._use_v2:
                from v2.nacos import ConfigParam

                content = self._run_coro(
                    self.client.get_config(
                        ConfigParam(data_id=self.nacos_config.dataid, group=self.nacos_config.group)
                    )
                )
            else:
                content = self.client.get_config(self.nacos_config.dataid, self.nacos_config.group)

            if not content:
                raise ValueError(
                    f"No configuration found in Nacos for dataid: {self.nacos_config.dataid}, "
                    f"group: {self.nacos_config.group}"
                )

            # Assume Nacos content is YAML
            raw_data = yaml.safe_load(content)

            # Expand environment variables in the configuration content
            expanded_data = expand_env_recursive(raw_data)

            return target_config_class(**expanded_data)
        except Exception as e:
            logger.error(f"Failed to load configuration from Nacos: {e}")
            raise
        finally:
            if self._use_v2 and self._loop is not None and not self._loop.is_closed():
                try:
                    self._run_coro(self._shutdown_v2_client())
                except Exception:
                    logger.debug("Error during v2 client shutdown", exc_info=True)
                finally:
                    self._loop.close()
                    self._loop = None