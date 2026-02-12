"""
测试接口定义
"""
import sys
import os
from abc import ABC

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_interface_imports():
    """测试接口导入"""
    try:
        from temporal_tree.interfaces import (
            INode, IIPProvider, IDataStore,
            IDimension, IQuery, IQueryBuilder
        )

        print("✓ 接口导入成功")

        # 验证接口是抽象基类
        assert issubclass(INode, ABC)
        assert issubclass(IIPProvider, ABC)
        assert issubclass(IDataStore, ABC)
        assert issubclass(IDimension, ABC)
        assert issubclass(IQuery, ABC)
        assert issubclass(IQueryBuilder, ABC)

        print("✓ 接口是抽象基类")

        return True

    except Exception as e:
        print(f"✗ 接口导入失败: {e}")
        return False


def test_interface_methods():
    """测试接口方法定义"""
    try:
        from temporal_tree.interfaces import INode

        # 获取INode的所有抽象方法
        methods = [name for name in dir(INode)
                   if not name.startswith('_') and callable(getattr(INode, name))]

        expected_methods = [
            'add_child', 'remove_child', 'get_child_by_ip',
            'get_child_by_name', 'get_data', 'set_data',
            'get_all_dimensions', 'has_dimension', 'get_descendants',
            'get_ancestors', 'to_dict', 'validate'
        ]

        for method in expected_methods:
            assert method in methods, f"缺少方法: {method}"

        print(f"✓ INode接口包含 {len(methods)} 个方法")
        print(f"✓ 包含所有预期方法")

        return True

    except Exception as e:
        print(f"✗ 接口方法验证失败: {e}")
        return False


def test_interface_properties():
    """测试接口属性定义"""
    try:
        from temporal_tree.interfaces import INode

        # 获取INode的所有属性
        properties = [name for name in dir(INode)
                      if not name.startswith('_') and not callable(getattr(INode, name))]

        expected_properties = [
            'node_id', 'name', 'level', 'ip_address',
            'parent', 'children'
        ]

        for prop in expected_properties:
            assert prop in properties, f"缺少属性: {prop}"

        print(f"✓ INode接口包含 {len(properties)} 个属性")
        print(f"✓ 包含所有预期属性")

        return True

    except Exception as e:
        print(f"✗ 接口属性验证失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    print("\n" + "=" * 60)
    print("测试接口定义")
    print("=" * 60)

    tests = [
        ("接口导入测试", test_interface_imports),
        ("接口方法测试", test_interface_methods),
        ("接口属性测试", test_interface_properties),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        if test_func():
            passed += 1

    print("\n" + "=" * 60)
    print(f"测试结果: {passed}/{total} 通过")
    print("=" * 60)

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)