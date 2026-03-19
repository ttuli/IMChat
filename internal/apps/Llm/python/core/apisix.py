import httpx
from .config import config

class ApisixClient:
    def __init__(self):
        self.admin_url = config.apisix_url
        self.api_key = config.apisix_admin_key
        self.headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json"
        }

    def register_upstream(self, upstream_id: str, host: str, port: int):
        """注册上游服务"""
        url = f"{self.admin_url}/apisix/admin/upstreams/{upstream_id}"
        data = {
            "name": upstream_id,
            "type": "roundrobin",
            "nodes": {
                f"{host}:{port}": 1
            },
            "scheme": "http",
            "pass_host": "pass"
        }
        resp = httpx.put(url, json=data, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def register_route(self, route_id: str, prefix: str, upstream_id: str):
        """注册路由"""
        url = f"{self.admin_url}/apisix/admin/routes/{route_id}"
        data = {
            "name": route_id,
            "uri": f"{prefix}/*",
            "upstream_id": upstream_id,
            "methods": ["GET", "POST", "PUT", "DELETE"]
        }
        resp = httpx.put(url, json=data, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def deregister(self, route_id: str, upstream_id: str):
        """注销服务"""
        httpx.delete(
            f"{self.admin_url}/apisix/admin/routes/{route_id}",
            headers=self.headers
        )
        httpx.delete(
            f"{self.admin_url}/apisix/admin/upstreams/{upstream_id}",
            headers=self.headers
        )