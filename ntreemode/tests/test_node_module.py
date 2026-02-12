"""
测试节点模块
"""
import sys
import os
from datetime import datetime

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_tree_node():
    """测试树节点实体"""
    from temporal_tree.core.node import TreeNode

    print("测试TreeNode类...")

    # 测试创建节点
    node = TreeNode(
        name="柴旦",
        ip_address="10.0.0.0",
        level=0
    )

    assert node.name == "柴旦"
    assert node.ip_address == "10.0.0.0"
    assert node.level == 0
    assert node.parent is None
    assert len(node.children) == 0
    print("✓ 节点创建测试通过")

    # 测试数据设置和获取
    node.set_data("standard_gas", 1000.0)
    node.set_data("meter_gas", 950.0)

    assert node.get_data("standard_gas") == 1000.0
    assert node.get_data("meter_gas") == 950.0
    assert node.has_dimension("standard_gas")
    assert not node.has_dimension("nonexistent")
    print("✓ 数据操作测试通过")

    # 测试父子关系
    child = TreeNode(
        name="上游结算",
        ip_address="10.0.0.0.1",
        level=1,
        parent=node
    )

    node.add_child(child)

    assert len(node.children) == 1
    assert node.children[0] == child
    assert child.parent == node
    print("✓ 父子关系测试通过")

    # 测试标签
    node.add_tag("region")
    node.add_tag("important")

    assert node.has_tag("region")
    assert node.has_tag("important")
    assert not node.has_tag("nonexistent")
    print("✓ 标签操作测试通过")

    # 测试转换为字典
    node_dict = node.to_dict()
    assert node_dict["name"] == "柴旦"
    assert node_dict["ip_address"] == "10.0.0.0"
    node_dict = node.to_dict()
    assert node_dict["name"] == "柴旦"
    assert node_dict["ip_address"] == "10.0.0.0"
    assert "children_count" in node_dict  # 这个字段总是存在的
    assert node_dict["children_count"] == 1  # 因为我们添加了一个子节点
    print("✓ 字典转换测试通过")

    # 如果需要测试包含子节点的字典
    node_dict_with_children = node.to_dict(include_children=True)
    assert "children" in node_dict_with_children
    assert len(node_dict_with_children["children"]) == 1
    print("✓ 包含子节点的字典转换测试通过")
    print("✓ 字典转换测试通过")

    return True


def test_node_factory():
    """测试节点工厂"""
    from temporal_tree.core.ip import IncrementalIPProvider
    from temporal_tree.core.node import NodeFactory

    print("\n测试NodeFactory类...")

    # 创建IP提供者和节点工厂
    ip_provider = IncrementalIPProvider(base_ip="10.0.0.0")
    factory = NodeFactory(ip_provider)

    # 测试创建根节点
    root = factory.create_root_node("燃气系统")
    assert root.name == "燃气系统"
    assert root.ip_address == "10.0.0.0"
    assert root.level == 0
    print("✓ 根节点创建测试通过")

    # 测试创建子节点
    child1 = factory.create_child_node(root, "区域A")
    assert child1.name == "区域A"
    assert child1.ip_address == "10.0.0.0.0"
    assert child1.level == 1
    assert child1.parent == root
    print("✓ 子节点创建测试通过")

    child2 = factory.create_child_node(root, "区域B")
    assert child2.name == "区域B"
    assert child2.ip_address == "10.0.0.0.1"
    print("✓ 第二个子节点创建测试通过")

    # 测试孙子节点
    grandchild = factory.create_child_node(child1, "站点A1")
    assert grandchild.name == "站点A1"
    assert grandchild.ip_address == "10.0.0.0.0.0"
    assert grandchild.level == 2
    print("✓ 孙子节点创建测试通过")

    # 测试节点查找
    found = factory.get_node(root.node_id)
    assert found == root

    found_by_ip = factory.get_node_by_ip("10.0.0.0.0")
    assert found_by_ip == child1
    print("✓ 节点查找测试通过")

    # 测试节点计数
    assert factory.get_node_count() == 4
    print("✓ 节点计数测试通过")

    return True


def test_node_repository():
    """测试节点仓库"""
    from temporal_tree.core.ip import IncrementalIPProvider
    from temporal_tree.core.node import NodeFactory, NodeRepository

    print("\n测试NodeRepository类...")

    # 创建测试树
    ip_provider = IncrementalIPProvider(base_ip="10.0.0.0")
    factory = NodeFactory(ip_provider)

    root = factory.create_root_node("燃气系统")
    region_a = factory.create_child_node(root, "区域A")
    region_b = factory.create_child_node(root, "区域B")
    site_a1 = factory.create_child_node(region_a, "站点A1")
    site_a2 = factory.create_child_node(region_a, "站点A2")

    # 创建仓库
    repository = NodeRepository(root)

    # 测试基本属性
    assert repository.root == root
    assert repository.get_node_count() == 5
    print("✓ 仓库初始化测试通过")

    # 测试节点查找
    found = repository.get_node(site_a1.node_id)
    assert found == site_a1

    found_by_ip = repository.get_node_by_ip("10.0.0.0.0.0")
    assert found_by_ip == site_a1
    print("✓ 仓库节点查找测试通过")

    # 测试条件查找
    regions = repository.find_nodes(level=1)
    assert len(regions) == 2
    assert region_a in regions
    assert region_b in regions
    print("✓ 条件查找测试通过")

    # 测试树遍历
    preorder = repository.traverse("preorder")
    assert len(preorder) == 5
    assert preorder[0] == root
    print("✓ 树遍历测试通过")

    # 测试树深度
    assert repository.get_tree_depth() == 2
    print("✓ 树深度计算测试通过")

    # 测试转换为字典
    tree_dict = repository.to_dict()
    assert tree_dict["node_count"] == 5
    assert tree_dict["tree_depth"] == 2
    print("✓ 树字典转换测试通过")

    return True


def test_node_data_operations():
    """测试节点数据操作"""
    from temporal_tree.core.node import TreeNode
    from datetime import datetime, timedelta

    print("\n测试节点数据操作...")

    # 创建测试节点
    node = TreeNode(
        name="测试站点",
        ip_address="10.0.0.0.0.0",
        level=2
    )

    # 测试时间序列数据
    time1 = datetime(2024, 1, 1, 10, 0, 0)
    time2 = datetime(2024, 1, 1, 11, 0, 0)
    time3 = datetime(2024, 1, 1, 12, 0, 0)

    node.set_data("standard_gas", 1000.0, time1)
    node.set_data("standard_gas", 1050.0, time2)
    node.set_data("standard_gas", 1100.0, time3)

    # 测试获取最新数据
    assert node.get_data("standard_gas") == 1100.0
    print("✓ 最新数据获取测试通过")

    # 测试获取特定时间数据
    assert node.get_data("standard_gas", time2) == 1050.0
    print("✓ 特定时间数据获取测试通过")

    # 测试计算维度
    def calculate_loss_rate(node, timestamp=None):
        standard = node.get_data("standard_gas", timestamp) or 0
        meter = node.get_data("meter_gas", timestamp) or 0
        if standard == 0:
            return 0
        return (standard - meter) / standard

    node.add_dimension_calculator("loss_rate", calculate_loss_rate)

    # 设置表计气量
    node.set_data("meter_gas", 950.0, time1)
    node.set_data("meter_gas", 980.0, time2)
    node.set_data("meter_gas", 1000.0, time3)

    # 测试计算维度
    loss_rate = node.get_data("loss_rate", time1)
    expected = (1000.0 - 950.0) / 1000.0
    assert abs(loss_rate - expected) < 0.001
    print("✓ 计算维度测试通过")

    # 测试所有维度
    dimensions = node.get_all_dimensions()
    assert "standard_gas" in dimensions
    assert "meter_gas" in dimensions
    assert "loss_rate" in dimensions
    print("✓ 维度列表测试通过")

    return True


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("测试节点模块")
    print("=" * 60)

    tests = [
        ("树节点实体测试", test_tree_node),
        ("节点工厂测试", test_node_factory),
        ("节点仓库测试", test_node_repository),
        ("节点数据操作测试", test_node_data_operations),
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