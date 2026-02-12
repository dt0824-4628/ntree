"""
测试时间模块
"""
import sys
import os
from datetime import datetime, timedelta

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_timeline():
    """测试时间线类"""
    from temporal_tree.core.time.timeline import Timeline, TimePoint

    print("测试Timeline类...")

    # 创建时间线
    timeline = Timeline(object_id="test_node", object_type="node")

    # 添加时间点
    time1 = datetime(2024, 1, 1, 10, 0, 0)
    timeline.add_time_point(time1, {"value": 100, "status": "normal"})

    time2 = datetime(2024, 1, 2, 10, 0, 0)
    timeline.add_time_point(time2, {"value": 110, "status": "warning"})

    time3 = datetime(2024, 1, 3, 10, 0, 0)
    timeline.add_time_point(time3, {"value": 120, "status": "normal"})

    # 测试基本功能
    assert len(timeline) == 3
    assert timeline.has_time_point(time1) is True
    assert timeline.get_latest().timestamp == time3

    print("✓ 时间线基本功能测试通过")

    # 测试时间范围查询
    points = timeline.get_time_range(
        start_time=datetime(2024, 1, 1, 12, 0, 0),
        end_time=datetime(2024, 1, 3, 0, 0, 0)
    )
    assert len(points) == 1  # 只包含time2

    print("✓ 时间范围查询测试通过")

    # 测试最接近时间点
    target_time = datetime(2024, 1, 2, 15, 0, 0)
    nearest = timeline.get_nearest_time_point(target_time)
    assert nearest.timestamp == time2

    print("✓ 最接近时间点测试通过")

    # 测试时间序列
    series = timeline.get_time_series("value")
    assert len(series) == 3
    assert series[0]["value"] == 100

    print("✓ 时间序列测试通过")

    # 测试字典转换
    timeline_dict = timeline.to_dict()
    assert timeline_dict["object_id"] == "test_node"
    assert "time_points" in timeline_dict

    # 测试从字典恢复
    restored = Timeline.from_dict(timeline_dict)
    assert len(restored) == 3

    print("✓ 字典转换测试通过")

    return True


def test_snapshot_system():
    """测试快照系统"""
    from temporal_tree.core.time.snapshot import SnapshotSystem
    from temporal_tree.core.node import TreeNode
    from temporal_tree.core.ip import IncrementalIPProvider

    print("\n测试SnapshotSystem类...")

    # 创建测试节点
    ip_provider = IncrementalIPProvider()
    node = TreeNode(
        name="测试节点",
        ip_address=ip_provider.allocate_root_ip(),
        level=0
    )

    # 设置一些数据
    node.set_data("standard_gas", 1000.0)
    node.set_data("meter_gas", 950.0)

    # 创建快照系统
    snapshot_system = SnapshotSystem()

    # 创建快照
    snapshot_id = snapshot_system.create_node_snapshot(
        node,
        comment="测试快照"
    )

    assert snapshot_id is not None
    print(f"✓ 创建快照成功: {snapshot_id}")

    # 获取快照列表
    snapshots = snapshot_system.get_node_snapshots(node.node_id)
    assert len(snapshots) == 1
    assert snapshots[0]["comment"] == "测试快照"

    print("✓ 快照列表查询测试通过")

    # 测试恢复快照
    node_state = snapshot_system.restore_node_snapshot(node.node_id, snapshot_id)
    assert node_state is not None
    assert node_state["name"] == "测试节点"

    print("✓ 快照恢复测试通过")

    # 创建多个时间点的快照
    times = [
        datetime(2024, 1, 1),
        datetime(2024, 2, 1),
        datetime(2024, 3, 1)
    ]

    for i, time in enumerate(times):
        node.set_data("standard_gas", 1000.0 + i * 100)
        snapshot_system.create_node_snapshot(
            node,
            timestamp=time,
            comment=f"第{i + 1}次快照"
        )

    # 查询时间范围内的快照
    jan_snapshots = snapshot_system.get_node_snapshots(
        node.node_id,
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 31)
    )
    assert len(jan_snapshots) == 1

    print("✓ 时间范围快照查询测试通过")

    return True


def test_integration():
    """测试时间模块集成"""
    from temporal_tree.core.time.timeline import Timeline
    from temporal_tree.core.time.snapshot import SnapshotSystem
    from temporal_tree.core.node import TreeNode, NodeRepository
    from temporal_tree.core.ip import IncrementalIPProvider

    print("\n测试时间模块集成...")

    # 创建测试树
    ip_provider = IncrementalIPProvider()

    root = TreeNode("根节点", ip_provider.allocate_root_ip(), 0)
    child1 = TreeNode("子节点1", ip_provider.allocate_child_ip(root.ip_address), 1, parent=root)
    child2 = TreeNode("子节点2", ip_provider.allocate_child_ip(root.ip_address), 1, parent=root)

    root.add_child(child1)
    root.add_child(child2)

    repository = NodeRepository(root)

    # 创建快照系统
    snapshot_system = SnapshotSystem()

    # 创建树快照
    tree_snapshot_id = snapshot_system.create_tree_snapshot(
        repository,
        comment="完整树快照"
    )

    print(f"✓ 树快照创建成功: {tree_snapshot_id}")

    # 为单个节点创建时间线
    timeline = Timeline(object_id=root.node_id, object_type="node")

    # 添加多个时间点的数据
    for i in range(5):
        time_point = datetime(2024, 1, i + 1, 10, 0, 0)
        root.set_data("standard_gas", 1000.0 + i * 50, time_point)

        timeline.add_time_point(
            time_point,
            {
                "standard_gas": root.get_data("standard_gas", time_point),
                "node_name": root.name
            }
        )

    # 测试时间线功能
    assert len(timeline) == 5
    assert timeline.get_time_range(
        start_time=datetime(2024, 1, 2),
        end_time=datetime(2024, 1, 4)
    ) == 3

    print("✓ 时间线与节点集成测试通过")

    # 测试时间序列分析
    gas_series = timeline.get_time_series("standard_gas")
    assert len(gas_series) == 5
    assert gas_series[-1]["value"] == 1200.0

    print("✓ 时间序列分析测试通过")

    return True


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("测试时间模块")
    print("=" * 60)

    tests = [
        ("时间线测试", test_timeline),
        ("快照系统测试", test_snapshot_system),
        ("集成测试", test_integration),
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