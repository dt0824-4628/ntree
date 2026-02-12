"""
测试 Timeline 与存储的集成
"""

import sys
import os
import pytest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree.data.storage.memory_store import MemoryStore
from temporal_tree.core.time.timeline import Timeline, TimePoint
from temporal_tree.exceptions import TimeError


class TestTimelineStorage:
    """测试 Timeline 的存储集成"""

    @pytest.fixture
    def storage(self):
        return MemoryStore()

    def test_timeline_auto_persist(self, storage):
        """测试自动持久化"""
        # 创建带存储的Timeline
        tl = Timeline(
            object_id="node_001",
            dimension="meter_gas",
            storage=storage,
            tree_id="tree_001"
        )

        # 添加时间点
        now = datetime.now()
        tl.add_time_point(now, 1500.5, unit="m³")

        # 验证存储中有了数据
        points = storage.get_time_points(
            tree_id="tree_001",
            node_id="node_001",
            dimension="meter_gas"
        )
        assert len(points) == 1
        assert points[0][1] == 1500.5

    def test_timeline_load_from_storage(self, storage):
        """测试从存储加载历史数据"""
        tree_id = "tree_001"
        node_id = "node_001"
        dim = "pressure"

        # 先直接往存储写一些历史数据
        t1 = datetime(2024, 1, 1)
        t2 = datetime(2024, 1, 2)
        storage.save_time_point(tree_id, node_id, dim, t1, 2.1)
        storage.save_time_point(tree_id, node_id, dim, t2, 2.2)

        # 创建Timeline，应该自动加载最近的数据
        tl = Timeline(
            object_id=node_id,
            dimension=dim,
            storage=storage,
            tree_id=tree_id,
            max_cache_size=100
        )

        # 验证内存缓存中有数据
        assert tl.get_time_point(t1) is not None
        assert tl.get_time_point(t2) is not None

    def test_timeline_cache_lru(self, storage):
        """测试LRU缓存淘汰（纯内存模式）"""
        # 不传storage，纯内存模式
        tl = Timeline(
            object_id="node_001",
            dimension="test",
            storage=None,  # ✅ 禁用存储，避免自动加载
            tree_id=None,
            max_cache_size=3
        )

        base = datetime(2024, 1, 1)

        # 添加3个点，缓存刚好满
        tl.add_time_point(base, 0)
        tl.add_time_point(base + timedelta(days=1), 1)
        tl.add_time_point(base + timedelta(days=2), 2)
        assert tl.size() == 3

        # 访问第1天，更新LRU顺序
        tl.get_time_point(base)

        # 添加第4个点，应该淘汰最久未访问的（第2天）
        tl.add_time_point(base + timedelta(days=3), 3)
        assert tl.size() == 3
        assert tl.get_time_point(base) is not None  # 第1天还在（刚访问过）
        assert tl.get_time_point(base + timedelta(days=1)) is None  # 第2天被淘汰
        assert tl.get_time_point(base + timedelta(days=2)) is not None  # 第3天还在
        assert tl.get_time_point(base + timedelta(days=3)) is not None  # 第4天还在

    def test_timeline_get_time_range(self, storage):
        """测试时间范围查询"""
        tl = Timeline(
            object_id="node_001",
            dimension="temperature",
            storage=storage,
            tree_id="tree_001"
        )

        # 添加30天的数据
        base = datetime(2024, 1, 1)
        for i in range(30):
            t = base + timedelta(days=i)
            tl.add_time_point(t, 20 + i)

        # 查询1月10-20日
        start = datetime(2024, 1, 10)
        end = datetime(2024, 1, 20)
        points = tl.get_time_range(start_time=start, end_time=end)

        assert len(points) == 11  # 10-20共11天
        assert points[0].timestamp == start
        assert points[-1].timestamp == end

    def test_timeline_delete_before(self, storage):
        """测试删除历史数据"""
        tl = Timeline(
            object_id="node_001",
            dimension="test",
            storage=storage,
            tree_id="tree_001"
        )

        # 添加10天的数据
        base = datetime(2024, 1, 1)
        for i in range(10):
            t = base + timedelta(days=i)
            tl.add_time_point(t, i)

        # 删除前5天
        deleted = tl.delete_before(datetime(2024, 1, 6))
        assert deleted == 5

        # 验证存储中也删除了
        remaining = storage.get_time_points(
            tree_id="tree_001",
            node_id="node_001",
            dimension="test"
        )
        assert len(remaining) == 5
        assert remaining[0][0] == datetime(2024, 1, 6)

    def test_timeline_no_storage(self):
        """测试无存储模式（纯内存）"""
        tl = Timeline(
            object_id="node_001",
            dimension="test"
        )

        now = datetime.now()
        tl.add_time_point(now, 100)

        assert tl.get_latest().value == 100
        assert len(tl.get_time_range()) == 1

    def test_timeline_get_latest_value(self, storage):
        """测试Timeline.get_latest()返回正确的value"""
        from datetime import datetime
        from temporal_tree.core.time.timeline import Timeline

        tl = Timeline(
            object_id="node_001",
            dimension="meter_gas",
            storage=storage,
            tree_id="tree_001"
        )

        now = datetime.now()
        tl.add_time_point(now, 1500.5)

        point = tl.get_latest()
        print(f"\n=== Debug Timeline.get_latest() ===")
        print(f"Point type: {type(point)}")
        print(f"Point value: {point.value}")
        print(f"Point dir: {[m for m in dir(point) if not m.startswith('_')]}")

        assert isinstance(point, TimePoint)  # 应该是TimePoint对象
        assert point.value == 1500.5  # value应该是1500.5