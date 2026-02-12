"""
最简单的接口测试
"""
import sys
import os

print("Python版本:", sys.version)
print("当前目录:", os.getcwd())

# 添加src到路径
src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
sys.path.insert(0, src_path)
print("添加路径:", src_path)

try:
    # 尝试导入
    from temporal_tree.interfaces import INode

    print("✓ 成功导入 INode")

    # 检查是否是抽象类
    from abc import ABC

    print("✓ INode 是 ABC 的子类:", issubclass(INode, ABC))

except Exception as e:
    print("✗ 导入失败:", type(e).__name__, "-", str(e))
    import traceback

    traceback.print_exc()