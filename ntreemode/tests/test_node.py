"""
测试节点模块 - 创建三层IP树
"""
import sys
import os
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree.core.ip.provider import IncrementalIPProvider
from temporal_tree.core.node.factory import NodeFactory
from temporal_tree.core.node.repository import NodeRepository


def create_three_level_tree():
    """创建三层IP树"""
    print("=" * 50)
    print("开始创建三层IP树")
    print("=" * 50)

    # 1. 创建IP分配器
    print("\n1. 创建IP分配器...")
    ip_provider = IncrementalIPProvider()
    print(f"   根IP: {ip_provider.allocate_root_ip()}")
    print(f"   最大深度: {ip_provider.get_max_depth()}")

    # 2. 创建节点工厂
    print("\n2. 创建节点工厂...")
    factory = NodeFactory(ip_provider)

    # 3. 创建根节点（第一层）
    print("\n3. 创建根节点...")
    root = factory.create_root_node(
        name="中国燃气总公司",
        metadata={"类型": "总公司", "成立时间": "2020-01-01"}
    )
    print(f"   根节点: {root.name}")
    print(f"   IP地址: {root.ip_address}")
    print(f"   节点ID: {root.node_id[:8]}...")

    # 4. 创建第二层节点
    print("\n4. 创建第二层节点...")
    north_china = factory.create_child_node(
        parent=root,
        name="华北分公司",
        metadata={"区域": "华北", "负责人": "张三"}
    )

    east_china = factory.create_child_node(
        parent=root,
        name="华东分公司",
        metadata={"区域": "华东", "负责人": "李四"}
    )

    print(f"   华北分公司: {north_china.ip_address}")
    print(f"   华东分公司: {east_china.ip_address}")

    # 5. 创建第三层节点
    print("\n5. 创建第三层节点...")
    beijing = factory.create_child_node(
        parent=north_china,
        name="北京市公司",
        metadata={"城市": "北京", "等级": "一线"}
    )

    tianjin = factory.create_child_node(
        parent=north_china,
        name="天津市公司",
        metadata={"城市": "天津", "等级": "一线"}
    )

    shanghai = factory.create_child_node(
        parent=east_china,
        name="上海市公司",
        metadata={"城市": "上海", "等级": "一线"}
    )

    print(f"   北京市公司: {beijing.ip_address}")
    print(f"   天津市公司: {tianjin.ip_address}")
    print(f"   上海市公司: {shanghai.ip_address}")

    return root, factory


def demonstrate_node_features(root, factory):
    """演示节点功能"""
    print("\n" + "=" * 50)
    print("演示节点核心功能")
    print("=" * 50)

    # 获取北京市节点
    beijing = factory.get_node_by_ip("10.0.0.0.0")
    if not beijing:
        print("❌ 找不到北京市节点")
        return

    print(f"\n1. 节点基本信息:")
    print(f"   名称: {beijing.name}")
    print(f"   IP地址: {beijing.ip_address}")
    print(f"   层级: {beijing.level}")
    print(f"   父节点: {beijing.parent.name if beijing.parent else '无'}")
    print(f"   子节点数: {len(beijing.children)}")

    # 2. 添加标签
    print("\n2. 添加标签...")
    beijing.add_tag("重点城市")
    beijing.add_tag("用气大户")
    print(f"   标签: {beijing.has_tag('重点城市')}")
    print(f"   标签: {beijing.has_tag('用气大户')}")

    # 3. 添加时间维度数据
    print("\n3. 添加时间维度数据...")
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    beijing.set_data("燃气用量", 1500.5, yesterday)
    beijing.set_data("燃气用量", 1600.2, today)
    beijing.set_data("管道压力", 0.8, today)

    print(f"   昨日用量: {beijing.get_data('燃气用量', yesterday)} m³")
    print(f"   今日用量: {beijing.get_data('燃气用量', today)} m³")
    print(f"   最新用量: {beijing.get_data('燃气用量')} m³")  # 不传时间，返回最新
    print(f"   管道压力: {beijing.get_data('管道压力')} MPa")

    # 4. 查看所有维度
    print("\n4. 节点所有维度:")
    for dimension in beijing.get_all_dimensions():
        print(f"   - {dimension}")

    # 5. 查看父子关系
    print("\n5. 父子关系链:")
    print(f"   当前节点: {beijing.name}")

    ancestors = beijing.get_ancestors()
    print(f"   祖先节点 ({len(ancestors)}个):")
    for ancestor in ancestors:
        print(f"     - {ancestor.name} (IP: {ancestor.ip_address})")

    # 6. 重命名节点
    print("\n6. 重命名节点...")
    old_name = beijing.name
    beijing.name = "北京市燃气总公司"
    print(f"   从 '{old_name}' 改为 '{beijing.name}'")


def demonstrate_repository_features(root):
    """演示仓库功能"""
    print("\n" + "=" * 50)
    print("演示仓库管理功能")
    print("=" * 50)

    # 1. 创建仓库
    print("\n1. 创建节点仓库...")
    repository = NodeRepository(root)

    print(f"   根节点: {repository.root.name}")
    print(f"   节点总数: {repository.get_node_count()}")
    print(f"   树深度: {repository.get_tree_depth()}")

    # 2. 遍历树
    print("\n2. 前序遍历（从上到下）:")
    nodes = repository.traverse("preorder")
    for i, node in enumerate(nodes, 1):
        indent = "  " * node.level
        print(f"   {i:2d}. {indent}{node.name} (IP: {node.ip_address})")

    # 3. 查找节点
    print("\n3. 查找节点:")

    # 按名称查找
    beijing_nodes = repository.find_nodes(name="北京市燃气总公司")
    print(f"   按名称查找 '北京市燃气总公司': {len(beijing_nodes)} 个")

    # 按层级查找
    level2_nodes = repository.find_nodes(level=2)
    print(f"   按层级查找 (level=2): {len(level2_nodes)} 个")

    # 4. 获取节点信息
    print("\n4. 获取节点信息:")
    all_nodes = repository.get_all_nodes()
    for node in all_nodes:
        print(f"   - {node.name}: IP={node.ip_address}, 层级={node.level}")

    # 5. 导出树结构
    print("\n5. 导出树结构（部分）:")
    tree_dict = repository.to_dict(include_data=True)
    print(f"   节点数量: {tree_dict['node_count']}")
    print(f"   树深度: {tree_dict['tree_depth']}")
    print(f"   根节点: {tree_dict['root']['name']}")


def main():
    """主函数"""
    try:
        # 创建三层树
        root, factory = create_three_level_tree()

        # 演示节点功能
        demonstrate_node_features(root, factory)

        # 演示仓库功能
        demonstrate_repository_features(root)

        print("\n" + "=" * 50)
        print("✅ 测试完成！成功演示了:")
        print("   • IP地址分配和层级编码")
        print("   • 节点创建和父子关系建立")
        print("   • 时间维度数据管理")
        print("   • 树形结构遍历和查询")
        print("=" * 50)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()