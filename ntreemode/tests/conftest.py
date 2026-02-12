"""
pytest配置文件
用于设置测试环境和共享fixtures
"""
import sys
import os

# 将src目录添加到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

print(f"Python路径: {sys.path}")
print(f"当前目录: {os.getcwd()}")