"""
基础测试
"""
import sys
import os

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_import():
    """测试导入"""
    from temporal_tree import TemporalTreeSystem
    assert TemporalTreeSystem is not None
    print("✓ 导入测试通过")


def test_system():
    """测试系统"""
    from temporal_tree import TemporalTreeSystem

    system = TemporalTreeSystem()
    assert system.name == "燃气输差分析系统"
    assert system.version == "1.0.0"

    # 测试树操作
    assert system.create_tree("tree1") == True
    assert system.create_tree("tree1") == False  # 重复创建失败
    assert system.create_tree("tree2") == True
    assert system.get_tree_count() == 2

    print("✓ 系统测试通过")


if __name__ == "__main__":
    print("运行测试...")
    test_import()
    test_system()
    print("所有测试通过！")
    