import os
import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv

# 加载项目根目录的 .env 文件
# index.py 位于 internal/apps/Llm/python/index.py，向上追溯四层到达根目录
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
env_path = os.path.join(base_dir, ".env")
load_dotenv(env_path)

from .core.config import config
from .router.suggestions import router as suggestions_router

app = FastAPI()

# 注册路由
app.include_router(suggestions_router, prefix="/ai", tags=["suggestions"])

def start():
    # 使用字符串形式的路径引入 app，以便 uvicorn 正确加载
    uvicorn.run("internal.apps.Llm.python.index:app", host=config.host, port=config.port)
