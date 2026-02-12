"""
简单的 Repository 存储/加载测试程序
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from temporal_tree.core.ip.provider import IncrementalIPProvider
from temporal_tree.core.node.factory import NodeFactory
from temporal_tree.core.node.repository import NodeRepository
from temporal_tree.data.storage.sqlite_store import SQLiteStore
from temporal_tree.data.storage.json_store import JSONStore
from temporal_tree.data.dimensions.registry import DimensionRegistry


def test_repository_storage():
    """测试 Repository 的存储和加载功能"""

    print("=" * 60)
    print("🧪 测试 Repository 存储/加载")
    print("=" * 60)

    # ===== 1. 初始化 =====
    print("\n[1/6] 初始化组件...")
    ip_provider = IncrementalIPProvider()
    factory = NodeFactory(ip_provider)
    storage = SQLiteStore("test_repo.db")
    print("✅ 初始化完成")

    # ===== 2. 创建测试树 =====
    print("\n[2/6] 创建测试树...")

    # 创建根节点
    root = factory.create_root_node("总公司")
    repo = NodeRepository(root)

    # 添加子节点
    beijing = factory.create_child_node(root, "北京公司")
    repo.add_node(beijing)

    shanghai = factory.create_child_node(root, "上海公司")
    repo.add_node(shanghai)

    chaoyang = factory.create_child_node(beijing, "朝阳门站")
    repo.add_node(chaoyang)

    haidian = factory.create_child_node(beijing, "海淀门站")
    repo.add_node(haidian)

    print(f"✅ 创建了 {len(repo.get_all_nodes())} 个节点")
    print(f"   根节点: {root.name} ({root.node_id})")
    print(f"   子节点: {beijing.name} ({beijing.node_id}), {shanghai.name} ({shanghai.node_id})")
    print(f"   北京下属: {chaoyang.name} ({chaoyang.node_id}), {haidian.name} ({haidian.node_id})")

    # 打印父子关系
    print("\n📋 父子关系:")
    for node in repo.get_all_nodes():
        parent_id = node.parent.node_id if node.parent else "None"
        print(f"   - {node.name} ({node.node_id}) -> 父节点: {parent_id}")

    # ===== 3. 添加测试数据 =====
    print("\n[3/6] 添加测试数据...")
    from datetime import datetime, timedelta

    now = datetime.now()
    for i, node in enumerate(repo.get_all_nodes()):
        node.set_data("meter_gas", 1000 + i * 100, now)
        node.set_data("standard_gas", 980 + i * 100, now)
        node.add_tag(f"level_{node.level}")
        node.add_tag(f"test_node")
    print(f"✅ 为 {len(repo.get_all_nodes())} 个节点添加了测试数据")

    # ===== 4. 保存到存储 =====
    print("\n[4/6] 保存到 SQLite...")
    repo.save_to_storage(storage, "test_tree_001")
    print("✅ 保存完成")

    # ===== 5. 从存储加载 =====
    print("\n[5/6] 从 SQLite 加载...")
    loaded_repo = NodeRepository.load_from_storage(storage, "test_tree_001")
    loaded_root = loaded_repo.root
    print("✅ 加载完成")

    # ===== 6. 验证 =====
    print("\n[6/6] 验证加载结果...")

    # 验证节点数量
    original_count = len(repo.get_all_nodes())
    loaded_count = len(loaded_repo.get_all_nodes())
    print(f"\n📊 节点数量对比:")
    print(f"   原树: {original_count} 个节点")
    print(f"   加载树: {loaded_count} 个节点")

    # 详细对比每个节点
    print(f"\n📋 原树节点列表:")
    original_nodes = {node.node_id: node for node in repo.get_all_nodes()}
    for node_id, node in original_nodes.items():
        parent_id = node.parent.node_id if node.parent else "None"
        print(f"   - {node_id[:8]}: {node.name} (层级: {node.level}, 父节点: {parent_id[:8] if parent_id != 'None' else 'None'})")

    print(f"\n📋 加载树节点列表:")
    loaded_nodes = {node.node_id: node for node in loaded_repo.get_all_nodes()}
    for node_id, node in loaded_nodes.items():
        parent_id = node.parent.node_id if node.parent else "None"
        print(f"   - {node_id[:8]}: {node.name} (层级: {node.level}, 父节点: {parent_id[:8] if parent_id != 'None' else 'None'})")

    # 找出差异
    original_ids = set(original_nodes.keys())
    loaded_ids = set(loaded_nodes.keys())

    missing_ids = original_ids - loaded_ids
    extra_ids = loaded_ids - original_ids

    if missing_ids:
        print(f"\n❌ 缺失的节点 ({len(missing_ids)} 个):")
        for node_id in missing_ids:
            node = original_nodes[node_id]
            print(f"   - {node_id[:8]}: {node.name}")

    if extra_ids:
        print(f"\n⚠️ 多余的节点 ({len(extra_ids)} 个):")
        for node_id in extra_ids:
            node = loaded_nodes[node_id]
            print(f"   - {node_id[:8]}: {node.name}")

    # 验证节点数量相等
    assert original_count == loaded_count, f"节点数量不一致: 原树 {original_count}, 加载树 {loaded_count}"

    # 验证根节点
    assert loaded_root.node_id == root.node_id, "根节点ID不一致"
    assert loaded_root.name == root.name, "根节点名称不一致"
    print(f"\n✓ 根节点验证通过: {loaded_root.name}")

    # 验证子节点数量
    assert len(loaded_root.children) == len(root.children), "子节点数量不一致"
    print(f"✓ 根节点子节点数量验证通过: {len(loaded_root.children)}")

    # 验证数据
    for node_id, original_node in original_nodes.items():
        loaded_node = loaded_repo.get_node(node_id)
        assert loaded_node is not None, f"节点 {node_id} 不存在于加载树中"

        # 验证名称
        assert loaded_node.name == original_node.name, f"节点 {node_id} 名称不一致"

        # 验证IP
        assert str(loaded_node.ip) == str(original_node.ip), f"节点 {node_id} IP不一致"

        # 验证层级
        assert loaded_node.level == original_node.level, f"节点 {node_id} 层级不一致"

        # 验证数据
        meter_value = loaded_node.get_data("meter_gas")
        original_meter = original_node.get_data("meter_gas")
        assert meter_value == original_meter, f"节点 {node_id} 表计气量不一致: {meter_value} vs {original_meter}"

        # 验证标签
        assert loaded_node.has_tag("test_node"), f"节点 {node_id} 缺少 test_node 标签"

    print(f"\n✓ 所有节点数据验证通过 ({original_count} 个节点)")

    # 清理
    import os
    if os.path.exists("test_repo.db"):
        os.remove("test_repo.db")
        print("\n🧹 清理测试文件")


def test_json_storage():
    """测试 JSON 存储"""

    print("\n" + "=" * 60)
    print("🧪 测试 JSON 存储")
    print("=" * 60)

    # ===== 1. 初始化 =====
    ip_provider = IncrementalIPProvider()
    factory = NodeFactory(ip_provider)
    storage = JSONStore("test_repo.json")

    # ===== 2. 创建测试树 =====
    root = factory.create_root_node("测试公司")
    repo = NodeRepository(root)

    child = factory.create_child_node(root, "测试部门")
    repo.add_node(child)

    # 添加数据
    from datetime import datetime
    now = datetime.now()
    root.set_data("meter_gas", 1500.5, now)
    child.set_data("standard_gas", 1480.3, now)

    print(f"\n📋 创建了 2 个节点:")
    print(f"   - {root.name} ({root.node_id[:8]})")
    print(f"   - {child.name} ({child.node_id[:8]})")

    # ===== 3. 保存 =====
    print("\n保存到 JSON...")
    repo.save_to_storage(storage, "test_json_tree")
    print("✅ 保存完成")

    # ===== 4. 加载 =====
    print("\n从 JSON 加载...")
    loaded_repo = NodeRepository.load_from_storage(storage, "test_json_tree")
    print("✅ 加载完成")

    # ===== 5. 验证 =====
    print("\n验证 JSON 加载结果...")

    assert loaded_repo.root.name == "测试公司", "根节点名称不一致"
    assert len(loaded_repo.root.children) == 1, "子节点数量不一致"
    assert loaded_repo.root.get_data("meter_gas") == 1500.5, "根节点数据不一致"

    child_node = loaded_repo.root.children[0]
    assert child_node.get_data("standard_gas") == 1480.3, "子节点数据不一致"

    print("✅ JSON 存储测试通过")

    # 清理
    import os
    if os.path.exists("test_repo.json"):
        os.remove("test_repo.json")
        print("🧹 清理测试文件")


if __name__ == "__main__":
    # 运行测试
    test_repository_storage()
    test_json_storage()

    print("\n" + "=" * 60)
    print("✨ 所有测试完成！")
    print("=" * 60)