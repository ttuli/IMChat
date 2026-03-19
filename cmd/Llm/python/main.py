import os
import sys

# 将工程根目录添加到 python 路径，以便导入 internal 中的模块
# 当前文件在 cmd/Llm/python/main.py，向上追溯三层到根目录
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.append(base_dir)

# 引入 index 模块中的启动方法
from internal.apps.Llm.python.index import start

if __name__ == "__main__":
    start()