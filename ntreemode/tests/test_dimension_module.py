# -*- coding: utf-8 -*-
"""
测试维度模块
"""
import sys
import os
from datetime import datetime

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_base_dimension():
    """测试维度基类"""
    from temporal_tree.data.dimensions.base import BaseDimension

    print("测试BaseDimension类...")

    # 创建测试维度子类
    class TestDimension(BaseDimension):
        def __init__(self):
            super().__init__(
                name="test_dim",
                display_name="测试维度",
                description="测试用维度",
                data_type=float,
                unit="unit",
                is_calculated=False
            )

        def _validate_impl(self, value):
            return isinstance(value, (int, float)) and value >= 0

        def _calculate_impl(self, node, timestamp=None):
            return 42.0  # 固定返回值

    # 测试维度创建
    dim = TestDimension()
    assert dim.name == "test_dim"
    assert dim.display_name == "测试维度"
    assert dim.data_type == float
    assert dim.unit == "unit"
    assert not dim.is_calculated
    print("✓ 维度属性测试通过")

    # 测试数据验证
    assert dim.validate(100.0) is True
    assert dim.validate(-10.0) is False  # 负值无效
    assert dim.validate("invalid") is False
    print("✓ 数据验证测试通过")

    # 测试数据格式化
    formatted = dim.format(1234.567)
    assert "1234.57" in formatted  # 两位小数
    assert "unit" in formatted
    print("✓ 数据格式化测试通过")

    # 测试默认值
    assert dim.get_default_value() == 0.0
    print("✓ 默认值测试通过")

    # 测试转换为字典
    dim_dict = dim.to_dict()
    assert dim_dict["name"] == "test_dim"
    assert dim_dict["display_name"] == "测试维度"
    print("✓ 字典转换测试通过")

    return True


def test_standard_gas_dimension():
    """测试标准气量维度"""
    from temporal_tree.data.dimensions import StandardGasDimension

    print("\n测试StandardGasDimension类...")

    dim = StandardGasDimension()

    # 测试基本属性
    assert dim.name == "standard_gas"
    assert dim.display_name == "标准气量"
    assert dim.unit == "m³"
    assert dim.data_type == float
    print("✓ 基本属性测试通过")

    # 测试数据验证
    assert dim.validate(1000.0) is True
    assert dim.validate(0.0) is True
    assert dim.validate(-100.0) is False  # 负值无效
    assert dim.validate(2000000.0) is False  # 超过最大值
    print("✓ 数据验证测试通过")

    # 测试有效范围
    valid_range = dim.get_valid_range()
    assert valid_range["min"] == 0.0
    assert valid_range["max"] == 1000000.0
    print("✓ 有效范围测试通过")

    # 测试数据格式化
    formatted = dim.format(1234567.89)
    assert "1,234,567.89" in formatted
    assert "标准气量:" in formatted
    print("✓ 数据格式化测试通过")

    return True


def test_meter_gas_dimension():
    """测试表计气量维度"""
    from temporal_tree.data.dimensions import MeterGasDimension

    print("\n测试MeterGasDimension类...")

    dim = MeterGasDimension()

    # 测试基本属性
    assert dim.name == "meter_gas"
    assert dim.display_name == "表计气量"
    assert dim.unit == "m³"
    print("✓ 基本属性测试通过")

    # 测试数据验证
    assert dim.validate(950.0) is True
    assert dim.validate(0.0) is True
    assert dim.validate(-50.0) is False
    print("✓ 数据验证测试通过")

    # 测试元数据
    metadata = dim.get_metadata()
    assert metadata["measurement_device"] == "gas_meter"
    assert metadata["accuracy"] == "±0.5%"
    print("✓ 元数据测试通过")

    return True


def test_loss_rate_dimension():
    """测试输差率维度"""
    from temporal_tree.data.dimensions import LossRateDimension

    print("\n测试LossRateDimension类...")

    dim = LossRateDimension()

    # 测试基本属性
    assert dim.name == "loss_rate"
    assert dim.display_name == "输差率"
    assert dim.unit == "%"
    assert dim.is_calculated is True
    print("✓ 基本属性测试通过")

    # 测试数据验证
    assert dim.validate(0.05) is True  # 5%
    assert dim.validate(-0.1) is True  # -10%
    assert dim.validate(1.5) is False  # 150%，超出范围
    assert dim.validate(-1.5) is False  # -150%，超出范围
    print("✓ 数据验证测试通过")

    # 测试输差等级判断
    assert dim.get_loss_level(0.02) == "normal"  # 2%
    assert dim.get_loss_level(0.06) == "warning"  # 6%
    assert dim.get_loss_level(0.15) == "alarm"  # 15%
    print("✓ 输差等级测试通过")

    # 测试数据格式化
    formatted = dim.format(0.0523)  # 5.23%
    assert "5.23%" in formatted
    assert "警告" in formatted
    print("✓ 数据格式化测试通过")

    return True


def test_dimension_registry():
    """测试维度注册表"""
    from temporal_tree.data.dimensions import DimensionRegistry

    print("\n测试DimensionRegistry类...")

    registry = DimensionRegistry()

    # 测试内置维度注册
    assert registry.has_dimension("standard_gas")
    assert registry.has_dimension("meter_gas")
    assert registry.has_dimension("loss_rate")
    print("✓ 内置维度注册测试通过")

    # 测试获取维度
    standard_dim = registry.get_dimension("standard_gas")
    assert standard_dim.name == "standard_gas"
    assert standard_dim.display_name == "标准气量"
    print("✓ 维度获取测试通过")

    # 测试维度列表
    dimensions = registry.list_dimensions()
    assert "standard_gas" in dimensions
    assert "meter_gas" in dimensions
    assert "loss_rate" in dimensions
    print(f"✓ 维度列表测试通过，共 {len(dimensions)} 个维度")

    # 测试维度信息
    dim_info = registry.list_dimensions_info()
    assert len(dim_info) == len(dimensions)
    for info in dim_info:
        assert "name" in info
        assert "display_name" in info
        assert "description" in info
    print("✓ 维度信息测试通过")

    # 测试数据验证
    assert registry.validate_dimension_data("standard_gas", 1000.0) is True
    assert registry.validate_dimension_data("standard_gas", -100.0) is False
    print("✓ 注册表数据验证测试通过")

    # 测试数据格式化
    formatted = registry.format_dimension_data("standard_gas", 1234.56)
    assert "1,234.56" in formatted
    print("✓ 注册表数据格式化测试通过")

    # 测试维度不存在的情况
    try:
        registry.get_dimension("nonexistent")
        assert False, "应该抛出异常"
    except Exception as e:
        assert "维度不存在" in str(e)
        print("✓ 维度不存在异常测试通过")

    return True


def test_calculation_integration():
    """测试计算维度集成"""
    from temporal_tree.core.node import TreeNode
    from temporal_tree.data.dimensions import DimensionRegistry
    from datetime import datetime

    print("\n测试计算维度集成...")

    # 创建测试节点
    node = TreeNode(
        name="测试站点",
        ip_address="10.0.0.0.0.0",
        level=2
    )

    # 设置气量数据
    timestamp = datetime(2024, 1, 1, 10, 0, 0)
    node.set_data("standard_gas", 1000.0, timestamp)
    node.set_data("meter_gas", 950.0, timestamp)

    # 创建注册表
    registry = DimensionRegistry()

    # 测试输差率计算
    loss_rate = registry.calculate_dimension("loss_rate", node, timestamp)
    expected = (1000.0 - 950.0) / 1000.0  # 0.05
    assert abs(loss_rate - expected) < 0.001
    print(f"✓ 输差率计算测试通过: {loss_rate:.3f}")

    # 测试节点添加计算器
    def calculate_loss_rate(n, t):
        return registry.calculate_dimension("loss_rate", n, t)

    node.add_dimension_calculator("loss_rate", calculate_loss_rate)

    # 通过节点获取计算维度
    node_loss_rate = node.get_data("loss_rate", timestamp)
    assert abs(node_loss_rate - expected) < 0.001
    print("✓ 节点计算器集成测试通过")

    # 测试无数据情况
    node2 = TreeNode(
        name="无数据站点",
        ip_address="10.0.0.0.0.1",
        level=2
    )

    loss_rate2 = registry.calculate_dimension("loss_rate", node2, timestamp)
    assert loss_rate2 is None
    print("✓ 无数据计算测试通过")

    # 测试除零情况
    node3 = TreeNode(
        name="零气量站点",
        ip_address="10.0.0.0.0.2",
        level=2
    )
    node3.set_data("standard_gas", 0.0, timestamp)
    node3.set_data("meter_gas", 0.0, timestamp)

    loss_rate3 = registry.calculate_dimension("loss_rate", node3, timestamp)
    assert loss_rate3 == 0.0
    print("✓ 除零处理测试通过")

    return True


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("测试维度模块")
    print("=" * 60)

    tests = [
        ("维度基类测试", test_base_dimension),
        ("标准气量维度测试", test_standard_gas_dimension),
        ("表计气量维度测试", test_meter_gas_dimension),
        ("输差率维度测试", test_loss_rate_dimension),
        ("维度注册表测试", test_dimension_registry),
        ("计算维度集成测试", test_calculation_integration),
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