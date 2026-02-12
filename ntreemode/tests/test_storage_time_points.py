"""
测试存储层的时间点存取接口
这个测试不依赖任何业务类，只测试存储接口
"""

import sys
import os
import pytest
from datetime import datetime, timedelta
from typing import List, Tuple, Any, Dict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree.data.storage.memory_store import MemoryStore
from temporal_tree.data.storage.json_store import JSONStore
from temporal_tree.data.storage.sqlite_store import SQLiteStore
from temporal_tree.exceptions import StorageError


class TestTimePointStorage:
    """测试所有存储实现的时间点存取功能"""

    @pytest.fixture(params=[
        'memory',
        'json',
        'sqlite'
    ])
    def storage(self, request, tmp_path):
        """参数化测试三种存储实现"""
        if request.param == 'memory':
            return MemoryStore()
        elif request.param == 'json':
            path = tmp_path / "test_data.json"
            return JSONStore(str(path))
        else:  # sqlite
            path = tmp_path / "test_data.db"
            return SQLiteStore(str(path))

    def test_save_and_get_time_point(self, storage):
        """测试保存和获取单个时间点"""
        # 准备数据
        now = datetime.now()

        # 保存时间点
        storage.save_time_point(
            tree_id="test_tree",
            node_id="10.0.0.1.1",
            dimension="meter_gas",
            timestamp=now,
            value=1500.5,
            quality=1,
            unit="m³"
        )

        # 获取时间点
        points = storage.get_time_points(
            tree_id="test_tree",
            node_id="10.0.0.1.1",
            dimension="meter_gas",
            start_time=now - timedelta(seconds=1),
            end_time=now + timedelta(seconds=1)
        )

        assert len(points) == 1
        ts, value, metadata = points[0]
        assert abs((ts - now).total_seconds()) < 1  # 时间近似相等
        assert value == 1500.5
        assert metadata.get('quality') == 1
        assert metadata.get('unit') == 'm³'

    def test_get_latest_time_point(self, storage):
        """测试获取最新的时间点"""
        node_id = "10.0.0.1.1"
        dimension = "pressure"

        # 保存三个不同时间的数据
        t1 = datetime(2024, 1, 1, 8, 0)
        t2 = datetime(2024, 1, 2, 8, 0)
        t3 = datetime(2024, 1, 3, 8, 0)

        storage.save_time_point("test", node_id, dimension, t1, 2.1)
        storage.save_time_point("test", node_id, dimension, t2, 2.2)
        storage.save_time_point("test", node_id, dimension, t3, 2.3)

        # 获取最新的
        latest = storage.get_latest_time_point("test", node_id, dimension)
        assert latest is not None
        ts, value, _ = latest
        assert ts == t3
        assert value == 2.3

        # 获取某个时间之前的最新
        latest_before = storage.get_latest_time_point(
            "test", node_id, dimension,
            before_time=t2 + timedelta(hours=1)
        )
        assert latest_before is not None
        ts, value, _ = latest_before
        assert ts == t2
        assert value == 2.2

    def test_time_range_query(self, storage):
        """测试时间范围查询"""
        node_id = "10.0.0.1.2"
        dimension = "standard_gas"

        # 保存2024年1月每天的数据
        base = datetime(2024, 1, 1)
        for i in range(31):
            t = base + timedelta(days=i)
            storage.save_time_point("test", node_id, dimension, t, 1000 + i)

        # 查询1月15-20日
        start = datetime(2024, 1, 15)
        end = datetime(2024, 1, 20)
        points = storage.get_time_points(
            "test", node_id, dimension,
            start_time=start,
            end_time=end
        )

        assert len(points) == 6  # 15-20共6天
        assert points[0][0] == start
        assert points[-1][0] == end

    def test_dimension_discovery(self, storage):
        """测试维度发现功能"""
        tree_id = "test_tree"
        node1 = "10.0.0.1.1"
        node2 = "10.0.0.1.2"

        # 节点1有气量和压强
        storage.save_time_point(tree_id, node1, "meter_gas", datetime.now(), 1500)
        storage.save_time_point(tree_id, node1, "pressure", datetime.now(), 2.5)

        # 节点2只有气量
        storage.save_time_point(tree_id, node2, "meter_gas", datetime.now(), 1800)

        # 获取所有维度
        all_dims = storage.get_dimensions(tree_id)
        assert set(all_dims) == {"meter_gas", "pressure"}

        # 获取节点1的维度
        node1_dims = storage.get_dimensions(tree_id, node1)
        assert set(node1_dims) == {"meter_gas", "pressure"}

        # 获取节点2的维度
        node2_dims = storage.get_dimensions(tree_id, node2)
        assert set(node2_dims) == {"meter_gas"}

    def test_time_range_discovery(self, storage):
        """测试时间范围发现"""
        tree_id = "test_tree"
        node_id = "10.0.0.1.1"
        dimension = "temperature"

        t1 = datetime(2024, 1, 1)
        t2 = datetime(2024, 12, 31)

        storage.save_time_point(tree_id, node_id, dimension, t1, 20)
        storage.save_time_point(tree_id, node_id, dimension, t2, 25)

        min_time, max_time = storage.get_time_range(tree_id, node_id, dimension)
        assert min_time == t1
        assert max_time == t2

    def test_delete_time_points(self, storage):
        """测试删除时间点"""
        tree_id = "test_tree"
        node_id = "10.0.0.1.1"
        dimension = "test_dim"

        # 保存10个点
        for i in range(10):
            t = datetime(2024, 1, 1) + timedelta(days=i)
            storage.save_time_point(tree_id, node_id, dimension, t, i)

        # 删除前5天的（1月1日-1月5日）
        deleted = storage.delete_time_points(
            tree_id=tree_id,
            node_id=node_id,
            dimension=dimension,
            before_time=datetime(2024, 1, 6)  # 不包含1月6日
        )
        assert deleted == 5  # 删除了1-5日共5条

        # 验证剩下的5条（1月6日-1月10日）
        remaining = storage.get_time_points(tree_id, node_id, dimension)
        assert len(remaining) == 5
        assert remaining[0][0] == datetime(2024, 1, 6)
        assert remaining[-1][0] == datetime(2024, 1, 10)

    def test_concurrent_dimensions(self, storage):
        """测试多维度同时写入"""
        tree_id = "test_tree"
        node_id = "10.0.0.1.1"
        now = datetime.now()

        # 同时写入多个维度
        dimensions = {
            "meter_gas": 1500.5,
            "standard_gas": 1480.3,
            "pressure": 2.5,
            "temperature": 25.0,
            "flow_rate": 120.5
        }

        for dim, value in dimensions.items():
            storage.save_time_point(tree_id, node_id, dim, now, value)

        # 验证每个维度都能独立查询
        for dim, expected in dimensions.items():
            points = storage.get_time_points(tree_id, node_id, dim)
            assert len(points) == 1
            assert points[0][1] == expected

        # 验证维度发现
        discovered = storage.get_dimensions(tree_id, node_id)
        assert set(discovered) == set(dimensions.keys())