"""
时间线模块 - 管理某个对象（节点/树）的某个维度的历史数据
每个Timeline代表一个维度的时间序列
"""

from datetime import datetime
from typing import Any, Optional, List, Tuple, Dict
from dataclasses import dataclass, field

from ...exceptions import TimeError
from ...data.storage.adapter import DataStoreAdapter


@dataclass
class TimePoint:
    """时间点数据"""
    timestamp: datetime
    value: Any
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """序列化"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'value': self.value,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'TimePoint':
        """反序列化"""
        return cls(
            timestamp=datetime.fromisoformat(data['timestamp']),
            value=data['value'],
            metadata=data.get('metadata', {})
        )


class Timeline:
    """
    时间线 - 管理某个对象（节点/树）的某个维度的历史数据

    职责：
    1. 内存缓存：最近访问的时间点数据
    2. 持久化：自动将新数据写入存储
    3. 查询：支持时间范围查询、最新值查询
    4. 缓存管理：LRU策略，避免内存溢出
    """

    def __init__(
        self,
        object_id: str,
        dimension: str,
        storage: Optional[DataStoreAdapter] = None,
        tree_id: Optional[str] = None,
        max_cache_size: int = 1000
    ):
        """
        初始化时间线

        Args:
            object_id: 对象ID（节点ID或树ID）
            dimension: 维度名称（如 'meter_gas', 'pressure'）
            storage: 存储适配器，如果提供则自动持久化
            tree_id: 所属树ID，用于存储查询
            max_cache_size: 内存缓存最大条目数
        """
        self.object_id = object_id
        self.dimension = dimension
        self._storage = storage
        self._tree_id = tree_id
        self._max_cache_size = max_cache_size

        # 内存缓存：时间戳 -> TimePoint
        self._time_points: Dict[datetime, TimePoint] = {}

        # LRU缓存淘汰用：按时间排序的key列表
        self._cache_order: List[datetime] = []

        # 如果提供了存储，预加载最近的数据
        if storage and tree_id:
            self._load_recent_points()

    def _load_recent_points(self, limit: int = 100):
        """从存储加载最近的时间点"""
        if not self._storage or not self._tree_id:
            return

        try:
            # 获取最近的点
            points = self._storage.get_time_points(
                tree_id=self._tree_id,
                node_id=self.object_id,
                dimension=self.dimension,
                limit=limit
            )

            for ts, value, metadata in points:
                self._time_points[ts] = TimePoint(ts, value, metadata)
                self._cache_order.append(ts)  # 按时间顺序添加

            # 确保不超过缓存大小
            self._ensure_cache_size()
        except Exception as e:
            # 存储出错不影响内存操作
            raise TimeError(f"加载历史数据失败: {e}")

    def _ensure_cache_size(self):
        print(f"当前缓存大小: {len(self._time_points)}, 最大: {self._max_cache_size}")
        print(f"缓存顺序: {[ts.day for ts in self._cache_order]}")

        while len(self._time_points) > self._max_cache_size:
            oldest = self._cache_order.pop(0)
            print(f"淘汰: {oldest.day}")
            if oldest in self._time_points:
                del self._time_points[oldest]

        print(f"淘汰后大小: {len(self._time_points)}")

    def add_time_point(
            self,
            timestamp: datetime,
            value: Any,
            metadata: Optional[Dict] = None,
            quality: int = 1,
            unit: Optional[str] = None,
            auto_persist: bool = True
    ) -> TimePoint:
        """添加时间点"""
        # 1. 构建元数据
        meta = metadata or {}
        if unit:
            meta['unit'] = unit
        meta['quality'] = quality
        meta['created_at'] = datetime.now().isoformat()

        # 2. 创建时间点
        point = TimePoint(timestamp, value, meta)

        # 🔍 添加调试
        print(f"🔍 TIMELINE ADD: timestamp={timestamp}, value={value}, type={type(value)}")

        # 3. 存入内存缓存
        if timestamp in self._time_points:
            if timestamp in self._cache_order:
                self._cache_order.remove(timestamp)

        self._time_points[timestamp] = point
        self._cache_order.append(timestamp)

        # ✅ 【关键】触发缓存淘汰！
        self._ensure_cache_size()

        # 4. 自动持久化
        if auto_persist and self._storage and self._tree_id:
            try:
                self._storage.save_time_point(
                    tree_id=self._tree_id,
                    node_id=self.object_id,
                    dimension=self.dimension,
                    timestamp=timestamp,
                    value=value,
                    quality=quality,
                    unit=unit
                )
                # 🔍 添加调试
                print(f"🔍 STORAGE SAVE: tree_id={self._tree_id}, node={self.object_id}, dim={self.dimension}")
            except Exception as e:
                raise TimeError(f"持久化时间点失败: {e}")

        return point
    def get_time_point(self, timestamp: datetime) -> Optional[TimePoint]:
        """
        获取指定时间点的数据

        策略：
        1. 先查内存缓存
        2. 没有再查存储
        3. 查到后加载到缓存
        4. 更新LRU顺序
        """
        # 1. 查内存
        if timestamp in self._time_points:
            # 【修复】更新LRU顺序：把访问的移到末尾
            if timestamp in self._cache_order:
                self._cache_order.remove(timestamp)
            self._cache_order.append(timestamp)
            return self._time_points[timestamp]

        # 2. 查存储
        if self._storage and self._tree_id:
            try:
                points = self._storage.get_time_points(
                    tree_id=self._tree_id,
                    node_id=self.object_id,
                    dimension=self.dimension,
                    start_time=timestamp,
                    end_time=timestamp,
                    limit=1
                )

                if points:
                    ts, value, metadata = points[0]
                    print(f"🔍 STORAGE RAW: value={value}, type={type(value)}")
                    print(f"🔍 STORAGE RAW: metadata={metadata}")
                    point = TimePoint(ts, value, metadata)
                    print(f"🔍 TIMEPOINT CREATED: point.value={point.value}, type={type(point.value)}")
                    self._time_points[ts] = point
                    self._cache_order.append(ts)
                    self._ensure_cache_size()
                    return point
            except Exception as e:
                raise TimeError(f"查询历史数据失败: {e}")

        return None

    def get_latest(self, before_time: Optional[datetime] = None) -> Optional[TimePoint]:
        # 1. 先从内存找
        candidates = []
        for ts, point in self._time_points.items():
            if before_time is None or ts < before_time:
                candidates.append((ts, point))

        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            point = candidates[0][1]
            print(f"DEBUG: get_latest from cache returns {type(point)}")  # 🐛
            return point

        # 2. 内存没有，查存储
        if self._storage and self._tree_id:
            try:
                latest = self._storage.get_latest_time_point(
                    tree_id=self._tree_id,
                    node_id=self.object_id,
                    dimension=self.dimension,
                    before_time=before_time
                )

                if latest:
                    ts, value, metadata = latest
                    point = TimePoint(ts, value, metadata)
                    print(f"DEBUG: get_latest from storage returns {type(point)}")  # 🐛
                    self._time_points[ts] = point
                    self._cache_order.append(ts)
                    self._ensure_cache_size()
                    return point
            except Exception as e:
                raise TimeError(f"查询最新数据失败: {e}")

        return None

    def get_time_range(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TimePoint]:
        """
        获取时间范围内的所有时间点

        策略：直接从存储查询，避免缓存不一致
        """
        if self._storage and self._tree_id:
            try:
                points = self._storage.get_time_points(
                    tree_id=self._tree_id,
                    node_id=self.object_id,
                    dimension=self.dimension,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit
                )

                result = []
                for ts, value, metadata in points:
                    point = TimePoint(ts, value, metadata)
                    result.append(point)
                    # 同时更新缓存
                    if ts not in self._time_points:
                        self._time_points[ts] = point
                        self._cache_order.append(ts)

                self._ensure_cache_size()
                return result
            except Exception as e:
                raise TimeError(f"查询时间范围失败: {e}")

        # 无存储时，从内存过滤
        result = []
        for ts, point in self._time_points.items():
            if start_time and ts < start_time:
                continue
            if end_time and ts > end_time:
                continue
            result.append(point)

        result.sort(key=lambda x: x.timestamp)
        if limit and limit > 0:
            result = result[:limit]

        return result

    def get_time_range_cached(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[TimePoint]:
        """
        仅从缓存获取时间范围（用于性能敏感场景）
        """
        result = []
        for ts, point in self._time_points.items():
            if start_time and ts < start_time:
                continue
            if end_time and ts > end_time:
                continue
            result.append(point)

        result.sort(key=lambda x: x.timestamp)
        return result

    def delete_before(self, before_time: datetime) -> int:
        """
        删除指定时间之前的所有点

        Returns:
            删除的数量
        """
        deleted_count = 0

        # 1. 删除内存中的
        to_delete = [ts for ts in self._time_points.keys() if ts < before_time]
        for ts in to_delete:
            del self._time_points[ts]
            if ts in self._cache_order:
                self._cache_order.remove(ts)
            deleted_count += 1

        # 2. 删除存储中的
        if self._storage and self._tree_id:
            try:
                deleted = self._storage.delete_time_points(
                    tree_id=self._tree_id,
                    node_id=self.object_id,
                    dimension=self.dimension,
                    before_time=before_time
                )
                deleted_count = max(deleted_count, deleted)
            except Exception as e:
                raise TimeError(f"删除历史数据失败: {e}")

        return deleted_count

    def clear_cache(self):
        """清空内存缓存（释放内存）"""
        self._time_points.clear()
        self._cache_order.clear()

    def size(self) -> int:
        """当前缓存大小"""
        return len(self._time_points)

    def to_dict(self) -> Dict:
        """序列化（只序列化数据，不序列化存储连接）"""
        return {
            'object_id': self.object_id,
            'dimension': self.dimension,
            'time_points': [
                point.to_dict() for point in self._time_points.values()
            ]
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict,
        storage: Optional[DataStoreAdapter] = None,
        tree_id: Optional[str] = None
    ) -> 'Timeline':
        """
        反序列化

        Args:
            data: 序列化数据
            storage: 存储适配器（反序列化后可以接入）
            tree_id: 树ID
        """
        timeline = cls(
            object_id=data['object_id'],
            dimension=data['dimension'],
            storage=storage,
            tree_id=tree_id
        )

        # 恢复内存缓存
        for point_data in data.get('time_points', []):
            point = TimePoint.from_dict(point_data)
            timeline._time_points[point.timestamp] = point
            timeline._cache_order.append(point.timestamp)

        return timeline

    def __len__(self) -> int:
        """历史数据总量（包括存储中的）"""
        if self._storage and self._tree_id:
            try:
                min_t, max_t = self._storage.get_time_range(
                    tree_id=self._tree_id,
                    node_id=self.object_id,
                    dimension=self.dimension
                )
                if min_t and max_t:
                    # 这里简化处理，实际应该查询COUNT
                    return len(self.get_time_range(limit=10000))
            except:
                pass
        return len(self._time_points)

    def __repr__(self) -> str:
        return f"Timeline(object={self.object_id}, dim={self.dimension}, cache={len(self._time_points)})"
