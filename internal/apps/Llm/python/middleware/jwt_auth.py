import time
from typing import Iterable

import jwt
from fastapi import Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware


class RedisJwtAuthMiddleware(BaseHTTPMiddleware):
    """JWT + Redis authentication middleware compatible with Go token manager."""

    CONTEXT_KEY_USER_ID = "user_id"

    def __init__(self, app, config, exclude_paths: Iterable[str] | None = None):
        super().__init__(app)
        self._exclude_paths = tuple(exclude_paths or ())

        jwt_secret = config.get_jwt_secret()
        redis_host = config.get_redis_host()
        redis_password = config.get_redis_password()

        if not jwt_secret:
            raise ValueError("TokenConfig.JWTConfig.Secret is required")
        if not redis_host:
            raise ValueError("TokenConfig.RedisConf.Host is required")

        self._jwt_secret = jwt_secret
        self._redis = Redis(
            host=redis_host.split(":")[0],
            port=int(redis_host.split(":")[1]) if ":" in redis_host else 6379,
            password=redis_password or None,
            decode_responses=True,
        )

    async def dispatch(self, request: Request, call_next):
        if self._is_excluded_path(request.url.path):
            return await call_next(request)

        token_string = self._extract_token(request)
        if not token_string:
            return self._unauthorized("身份已过期")

        user_id, token_key, err_msg = self._validate_jwt_and_build_key(token_string)
        if err_msg:
            return self._unauthorized(err_msg)

        stored_token = await self._redis.get(token_key)
        if not stored_token or stored_token != token_string:
            return self._unauthorized("账号已在其他设备登录，您已被强制下线")

        request.state.user_id = user_id
        return await call_next(request)

    def _is_excluded_path(self, path: str) -> bool:
        for exclude in self._exclude_paths:
            if path == exclude or path.startswith(f"{exclude}/"):
                return True
        return False

    @staticmethod
    def _extract_token(request: Request) -> str:
        auth_header = request.headers.get("Authorization", "")
        if auth_header:
            parts = auth_header.split(" ", 1)
            if len(parts) == 2 and parts[0] == "Bearer":
                return parts[1]
        return request.query_params.get("token", "")

    def _validate_jwt_and_build_key(self, token_string: str):
        try:
            payload = jwt.decode(
                token_string,
                self._jwt_secret,
                algorithms=["HS256"],
                options={"verify_exp": False},
            )
        except jwt.PyJWTError:
            return 0, "", "身份已过期"

        exp = payload.get("exp")
        if not isinstance(exp, (int, float)):
            return 0, "", "身份已过期"
        if int(time.time()) > int(exp):
            return 0, "", "身份已过期"

        user_id_raw = payload.get(self.CONTEXT_KEY_USER_ID)
        try:
            user_id = int(user_id_raw)
        except (TypeError, ValueError):
            return 0, "", "身份已过期"

        token_type = payload.get("token_type")
        if token_type == "access":
            token_type_short = "at"
        elif token_type == "refresh":
            token_type_short = "rt"
        else:
            return 0, "", "身份已过期"

        device_id = payload.get("device_id")
        if not isinstance(device_id, str):
            device_id = ""

        token_key = f"token:{user_id}:{device_id}:{token_type_short}"
        return user_id, token_key, ""

    @staticmethod
    def _unauthorized(message: str) -> JSONResponse:
        return JSONResponse(status_code=401, content={"code": 401, "message": message})
