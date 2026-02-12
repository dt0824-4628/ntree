"""
测试系统基本功能
"""
import sys
import os

# 调试信息
print("=" * 50)
print(f"Python版本: {sys.version}")
print(f"当前目录: {os.getcwd()}")
print(f"sys.path: {sys.path}")
print("=" * 50)


def test_import():
    """测试导入"""
    try:
        from temporal_tree import TemporalTreeSystem
        print("✓ 导入成功")
        return True
    except ImportError as e:
        print(f"✗ 导入失败: {e}")
        return False


def test_system_creation():
    """测试系统创建"""
    try:
        from temporal_tree import TemporalTreeSystem
        system = TemporalTreeSystem()
        assert system.name == "燃气输差分析系统"
        assert system.version == "1.0.0"
        print("✓ 系统创建成功")
        return True
    except Exception as e:
        print(f"✗ 系统创建失败: {e}")
        return False


def test_tree_operations():
    """测试树操作"""
    try:
        from temporal_tree import TemporalTreeSystem
        system = TemporalTreeSystem()

        # 创建树
        result = system.create_tree("test_tree_1")
        assert result is True
        assert system.get_tree_count() == 1

        # 重复创建应失败
        result = system.create_tree("test_tree_1")
        assert result is False

        # 创建第二棵树
        system.create_tree("test_tree_2")
        assert system.get_tree_count() == 2

        print("✓ 树操作测试成功")
        return True
    except Exception as e:
        print(f"✗ 树操作测试失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    print("\n" + "=" * 50)
    print("开始运行测试...")
    print("=" * 50)

    tests = [
        ("导入测试", test_import),
        ("系统创建测试", test_system_creation),
        ("树操作测试", test_tree_operations),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        if test_func():
            passed += 1

    print("\n" + "=" * 50)
    print(f"测试结果: {passed}/{total} 通过")
    print("=" * 50)

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)