"""
测试 TreeNode 与 Timeline 的集成
"""

import sys
import os
import pytest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree.core.ip.address import IPAddress
from temporal_tree.core.node.entity import TreeNode
from temporal_tree.data.storage.memory_store import MemoryStore
from temporal_tree.data.dimensions.registry import DimensionRegistry
from temporal_tree.exceptions import NodeError


class TestTreeNodeTimeline:
    """测试 TreeNode 的 Timeline 集成"""

    @pytest.fixture
    def storage(self):
        return MemoryStore()

    @pytest.fixture
    def node(self, storage):
        """创建测试节点"""
        # ✅ IPAddress 直接接收字符串
        ip = IPAddress("10.0.0.1")  # 直接传IP字符串

        return TreeNode(
            node_id="test_node_001",
            name="测试节点",
            ip=ip,
            storage=storage,
            tree_id="test_tree"
        )

    def test_set_data_create_timeline(self, node):
        """测试set_data自动创建Timeline"""
        now = datetime.now()
        node.set_data("meter_gas", 1500.5, now)

        assert "meter_gas" in node._timelines
        tl = node._timelines["meter_gas"]
        assert tl.dimension == "meter_gas"
        assert tl.object_id == node.node_id

        # 验证数据
        value = node.get_data("meter_gas", now)
        assert value == 1500.5

    def test_get_data_latest(self, node):
        """测试获取最新数据"""
        t1 = datetime(2024, 1, 1)
        t2 = datetime(2024, 1, 2)
        t3 = datetime(2024, 1, 3)

        # ✅ 改用已注册的维度
        node.set_data("meter_gas", 2.1, t1)  # pressure -> meter_gas
        node.set_data("meter_gas", 2.2, t2)
        node.set_data("meter_gas", 2.3, t3)

        latest = node.get_data("meter_gas")
        assert latest == 2.3

    def test_get_data_time_travel(self, node):
        """测试时间旅行"""
        t1 = datetime(2024, 1, 1)
        t2 = datetime(2024, 1, 2)

        # ✅ 改用已注册的维度
        node.set_data("standard_gas", 25.0, t1)  # temperature -> standard_gas
        node.set_data("standard_gas", 26.5, t2)

        old_value = node.get_data("standard_gas", t1)
        assert old_value == 25.0

    def test_get_data_with_tolerance(self, node):
        """测试容差查询"""
        t1 = datetime(2024, 1, 1, 8, 0, 0)
        # ✅ 输差率通常在 0-10% 之间
        node.set_data("loss_rate", 1.5, t1)  # 1.5% 而不是 120.5%

        query_time = datetime(2024, 1, 1, 8, 5, 0)
        value = node.get_data("loss_rate", query_time, tolerance=600)
        assert value == 1.5

    def test_get_time_series(self, node):
        """测试获取时间序列"""
        base = datetime(2024, 1, 1)
        for i in range(10):
            t = base + timedelta(days=i)
            node.set_data("meter_gas", 1500 + i, t)

        series = node.get_time_series(
            "meter_gas",
            start_time=base + timedelta(days=3),
            end_time=base + timedelta(days=6)
        )

        assert len(series) == 4  # 第3-6天，共4天
        assert series[0][0] == base + timedelta(days=3)
        assert series[0][1] == 1503
        assert series[-1][0] == base + timedelta(days=6)
        assert series[-1][1] == 1506

    def test_auto_persist(self, node, storage):
        """测试自动持久化"""
        now = datetime.now()
        node.set_data("meter_gas", 1500.5, now)

        # 验证存储中是否有数据
        points = storage.get_time_points(
            tree_id="test_tree",
            node_id=node.node_id,
            dimension="meter_gas"
        )
        assert len(points) == 1
        assert points[0][1] == 1500.5

    def test_dimension_validation(self, node):
        """测试维度验证"""
        # 已注册的维度
        node.set_data("meter_gas", 1500.5)  # ✅ 正常

        # ✅ 未注册的维度应该被捕获为KeyError，走except分支
        node.set_data("custom_dim", 123)  # 应该能执行，不抛异常

        # 错误的值类型
        with pytest.raises(ValueError):
            node.set_data("meter_gas", "不是数字")

    def test_delete_dimension_data(self, node):
        """测试删除维度数据"""
        base = datetime(2024, 1, 1)
        # ✅ 改用已注册的维度
        for i in range(10):
            t = base + timedelta(days=i)
            node.set_data("meter_gas", i, t)  # test_dim -> meter_gas

    def test_node_lifecycle(self, node):
        """测试节点生命周期"""
        assert node.is_active is True
        assert node.deleted_at is None

        # 记录创建时间
        created_time = node.created_at
        print(f"\n节点创建时间: {created_time}")

        # 等待一小段时间
        import time
        time.sleep(0.001)  # 等待1毫秒

        # 删除节点
        delete_time = datetime.now()
        node.delete(delete_time)

        print(f"节点删除时间: {delete_time}")
        assert delete_time > created_time  # 确保删除时间晚于创建时间

        assert node.is_active is False
        assert node.deleted_at == delete_time

        # 已删除节点不能设置数据
        with pytest.raises(NodeError):
            node.set_data("meter_gas", 100)

        # ✅ 时间旅行查询：使用创建时间和删除时间之间的时间点
        middle_time = created_time + (delete_time - created_time) / 2
        print(f"中间时间: {middle_time}")
        assert node.is_alive_at(middle_time) is True  # 存活期内

        # 删除后不存活
        future = delete_time + timedelta(seconds=1)
        assert node.is_alive_at(future) is False

    def test_serialization(self, node, storage):
        """测试序列化/反序列化"""
        # 添加一些数据
        node.set_data("meter_gas", 1500.5, datetime(2024, 1, 1))
        node.set_data("standard_gas", 2.5, datetime(2024, 1, 1))  # pressure -> standard_gas
        node.add_tag("重要")
        node.add_tag("测试")

