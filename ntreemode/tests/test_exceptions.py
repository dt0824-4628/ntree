"""
测试异常体系
"""
import sys
import os

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_exception_import():
    """测试异常导入"""
    try:
        from temporal_tree.exceptions import (
            BaseError, InvalidIPFormatError, IPAllocationError,
            NodeNotFoundError, TreeNotFoundError
        )
        print("✓ 异常导入成功")
        return True
    except Exception as e:
        print(f"✗ 异常导入失败: {e}")
        return False


def test_exception_creation():
    """测试异常创建"""
    try:
        from temporal_tree.exceptions import InvalidIPFormatError

        # 测试创建异常
        error = InvalidIPFormatError(
            ip_address="256.0.0.1",
            reason="段值超出范围"
        )

        assert error.code == "INVALID_IP_FORMAT"
        assert "256.0.0.1" in str(error)
        assert "段值超出范围" in str(error)
        assert error.details["ip_address"] == "256.0.0.1"
        assert error.details["reason"] == "段值超出范围"

        print("✓ 异常创建测试通过")
        return True
    except Exception as e:
        print(f"✗ 异常创建测试失败: {e}")
        return False


def test_exception_inheritance():
    """测试异常继承关系"""
    try:
        from temporal_tree.exceptions import (
            BaseError, IPError, InvalidIPFormatError
        )

        # 测试继承关系
        error = InvalidIPFormatError("10.0.0.256")
        assert isinstance(error, IPError)
        assert isinstance(error, BaseError)
        assert issubclass(InvalidIPFormatError, IPError)
        assert issubclass(InvalidIPFormatError, BaseError)

        print("✓ 异常继承关系测试通过")
        return True
    except Exception as e:
        print(f"✗ 异常继承关系测试失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("测试异常体系")
    print("=" * 60)

    tests = [
        ("异常导入测试", test_exception_import),
        ("异常创建测试", test_exception_creation),
        ("异常继承测试", test_exception_inheritance),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ 测试失败: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"测试结果: {passed}/{total} 通过")
    print("=" * 60)

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)