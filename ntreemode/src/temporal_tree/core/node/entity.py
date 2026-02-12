"""
树节点实体模块
定义树节点，每个节点代表组织架构中的一个实体
"""

from typing import Optional, Dict, Any, List, Set, Union
from datetime import datetime, timedelta  # 加上 timedelta

from ..ip.address import IPAddress
from ...data.dimensions.registry import DimensionRegistry
from ...data.storage.adapter import DataStoreAdapter
from ..time.timeline import Timeline
from ...exceptions import NodeError, DimensionNotFoundError


class TreeNode:
    """
    树节点 - 代表组织架构中的一个实体

    每个节点包含：
    1. 身份信息：node_id, name, ip, level
    2. 树关系：parent, children
    3. 维度数据：每个维度一个Timeline，支持时间旅行
    4. 标签系统：用于快速分类和查询
    """

    def __init__(
        self,
        node_id: str,
        name: str,
        ip: IPAddress,
        level: int = 0,
        storage: Optional[DataStoreAdapter] = None,
        tree_id: Optional[str] = None,
        max_cache_size: int = 1000
    ):
        """
        初始化树节点

        Args:
            node_id: 节点唯一标识
            name: 节点名称
            ip: IP地址（增量编码）
            level: 层级深度
            storage: 存储适配器（用于持久化）
            tree_id: 所属树ID
            max_cache_size: 每个维度的最大缓存点数
        """
        # ========== 身份信息 ==========
        self.node_id = node_id
        self.name = name
        self.ip = ip
        self.level = level

        # ========== 存储配置 ==========
        self._storage = storage
        self._tree_id = tree_id
        self._max_cache_size = max_cache_size

        # ========== 树结构关系 ==========
        self.parent: Optional['TreeNode'] = None
        self.children: List['TreeNode'] = []

        # ========== 标签系统 ==========
        self._tags: Set[str] = set()

        # ========== 维度数据（✅ 改造点） ==========
        # 每个维度一个Timeline，支持时间旅行和自动持久化
        self._timelines: Dict[str, Timeline] = {}

        # ========== 生命周期管理 ==========
        self.created_at: datetime = datetime.now()
        self.deleted_at: Optional[datetime] = None
        self.is_active: bool = True

    # ========== 维度数据管理 ==========

    def _get_or_create_timeline(self, dimension: str) -> Timeline:
        """
        获取或创建指定维度的Timeline

        Args:
            dimension: 维度名称

        Returns:
            Timeline对象
        """
        if dimension not in self._timelines:
            self._timelines[dimension] = Timeline(
                object_id=self.node_id,
                dimension=dimension,
                storage=self._storage,
                tree_id=self._tree_id,
                max_cache_size=self._max_cache_size
            )
        return self._timelines[dimension]

    def set_data(
        self,
        dimension: str,
        value: Any,
        timestamp: Optional[datetime] = None,
        quality: int = 1,
        unit: Optional[str] = None,
        auto_persist: bool = True
    ) -> None:
        """
        设置维度数据

        Args:
            dimension: 维度名称（如 'meter_gas', 'pressure'）
            value: 数值
            timestamp: 时间戳，默认当前时间
            quality: 质量码（0=无效,1=正常,2=估算）
            unit: 单位（覆盖维度默认单位）
            auto_persist: 是否自动持久化

        Raises:
            NodeError: 节点已删除时设置数据
            ValueError: 数据验证失败
        """
        # 1. 检查节点状态
        if not self.is_active:
            raise NodeError(f"节点已删除，无法设置数据: {self.node_id}")

        # 2. 数据验证
        try:
            dim = DimensionRegistry().get_dimension(dimension)
            validated_value = dim.validate(value)
            actual_unit = unit or dim.unit
        except (KeyError, DimensionNotFoundError):  # ✅ 同时捕获两种异常
            # 维度不存在时，只做基本类型检查
            validated_value = value
            actual_unit = unit
        except Exception as e:
            raise ValueError(f"数据验证失败 [{dimension}]: {e}")
        # 3. 获取或创建Timeline
        tl = self._get_or_create_timeline(dimension)

        # 4. 记录时间点
        ts = timestamp or datetime.now()
        tl.add_time_point(
            timestamp=ts,
            value=validated_value,
            quality=quality,
            unit=actual_unit,
            auto_persist=auto_persist
        )

    def get_data(self, dimension: str, timestamp: Optional[datetime] = None, tolerance: Optional[int] = None) -> \
            Optional[Any]:
        # ========== 1. 处理计算型维度 ==========
        try:
            dim = DimensionRegistry().get_dimension(dimension)
            if dim.is_calculated:
                # 输差率计算
                if dimension == "loss_rate":
                    # 获取计算所需的基础数据
                    standard = self.get_data("standard_gas", timestamp, tolerance)
                    meter = self.get_data("meter_gas", timestamp, tolerance)

                    # 只有两个数据都存在时才计算
                    if standard is not None and meter is not None:
                        return dim.calculate(standard, meter)
                    return None
                # 未来可以添加其他计算型维度
                return None
        except:
            # 维度不存在或不是计算型，继续走存储型逻辑
            pass
        if dimension not in self._timelines:
            return None

        tl = self._timelines[dimension]

        if timestamp is None:
            point = tl.get_latest()
            print(f"🔍 DEBUG: get_latest() returned {type(point)}")  # 强制输出
            if point:
                print(f"🔍 DEBUG: point.value = {point.value}")
                return point.value
            return None

        point = tl.get_time_point(timestamp)
        if point:
            print(f"🔍 DEBUG: get_time_point() returned value={point.value}")
            return point.value

        # 容差查询
        if tolerance:
            start = timestamp - timedelta(seconds=tolerance)
            end = timestamp + timedelta(seconds=tolerance)
            points = tl.get_time_range(start_time=start, end_time=end, limit=1)
            if points:
                # ✅ 调试代码放在这里！
                print(f"🔍 TOLERANCE QUERY: points[0].value={points[0].value}, type={type(points[0].value)}")
                return points[0].value

        return None

    def get_time_series(
        self,
        dimension: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[tuple]:
        """
        获取时间序列数据

        Returns:
            List of (timestamp, value)
        """
        if dimension not in self._timelines:
            return []

        tl = self._timelines[dimension]
        points = tl.get_time_range(
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

        return [(p.timestamp, p.value) for p in points]

    def get_dimensions(self) -> List[str]:
        """
        获取节点所有有数据的维度

        Returns:
            维度名称列表
        """
        return list(self._timelines.keys())

    def delete_dimension_data(
        self,
        dimension: str,
        before_time: Optional[datetime] = None
    ) -> int:
        """
        删除维度数据

        Args:
            dimension: 维度名称
            before_time: 删除此时间之前的数据，None表示删除所有

        Returns:
            删除的数据点数
        """
        if dimension not in self._timelines:
            return 0

        tl = self._timelines[dimension]
        deleted = tl.delete_before(before_time) if before_time else len(tl)

        if before_time is None or deleted == len(tl):
            # 删除整个维度
            del self._timelines[dimension]

        return deleted

    # ========== 标签管理 ==========

    def add_tag(self, tag: str) -> None:
        """添加标签"""
        self._tags.add(tag)

    def remove_tag(self, tag: str) -> None:
        """移除标签"""
        self._tags.discard(tag)

    def has_tag(self, tag: str) -> bool:
        """检查是否有标签"""
        return tag in self._tags

    def get_tags(self) -> List[str]:
        """获取所有标签"""
        return sorted(list(self._tags))

    # ========== 树结构管理 ==========

    def add_child(self, child_node: 'TreeNode') -> None:
        """添加子节点"""
        if child_node not in self.children:
            self.children.append(child_node)
            child_node.parent = self

    def remove_child(self, child_node: 'TreeNode') -> bool:
        """移除子节点"""
        if child_node in self.children:
            self.children.remove(child_node)
            child_node.parent = None
            return True
        return False

    def get_ancestors(self) -> List['TreeNode']:
        """获取所有祖先节点（从根到父节点）"""
        ancestors = []
        current = self.parent
        while current:
            ancestors.insert(0, current)
            current = current.parent
        return ancestors

    def get_descendants(self) -> List['TreeNode']:
        """获取所有后代节点（递归）"""
        descendants = []
        for child in self.children:
            descendants.append(child)
            descendants.extend(child.get_descendants())
        return descendants

    def get_root(self) -> 'TreeNode':
        """获取根节点"""
        root = self
        while root.parent:
            root = root.parent
        return root

    def get_path(self) -> List[str]:
        """获取从根到当前节点的路径名称"""
        path = [self.name]
        current = self.parent
        while current:
            path.insert(0, current.name)
            current = current.parent
        return path

    # ========== 生命周期管理 ==========

    def delete(self, timestamp: Optional[datetime] = None) -> None:
        """
        软删除节点
        节点标记为已删除，但历史数据保留
        """
        self.deleted_at = timestamp or datetime.now()
        self.is_active = False

    def is_alive_at(self, timestamp: datetime) -> bool:
        """
        判断节点在指定时间点是否存活

        Args:
            timestamp: 时间点

        Returns:
            True 表示节点在该时间点存在
        """
        if timestamp < self.created_at:
            return False
        if self.deleted_at and timestamp >= self.deleted_at:
            return False
        return True

    # ========== 序列化 ==========

    def to_dict(self, include_children: bool = True, include_data: bool = True) -> Dict[str, Any]:
        """
        序列化节点

        Args:
            include_children: 是否包含子节点ID列表
            include_data: 是否包含维度数据

        Returns:
            可JSON序列化的字典
        """
        result = {
            'node_id': self.node_id,
            'name': self.name,
            'ip': str(self.ip),
            'level': self.level,
            'tags': list(self._tags),
            'created_at': self.created_at.isoformat(),
            'deleted_at': self.deleted_at.isoformat() if self.deleted_at else None,
            'is_active': self.is_active,
            'parent_id': self.parent.node_id if self.parent else None,  # ✅ 必须保存！
        }

        if include_children:
            result['children'] = [child.node_id for child in self.children]

        if include_data:
            result['timelines'] = {
                dim: tl.to_dict()
                for dim, tl in self._timelines.items()
            }

        return result

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        storage: Optional[DataStoreAdapter] = None,
        tree_id: Optional[str] = None,
        max_cache_size: int = 1000
    ) -> 'TreeNode':
        """
        反序列化创建节点

        Args:
            data: 序列化的节点数据
            storage: 存储适配器
            tree_id: 所属树ID
            max_cache_size: 缓存大小
        """
        node = cls(
            node_id=data['node_id'],
            name=data['name'],
            ip = IPAddress(data['ip']),
            level=data['level'],
            storage=storage,
            tree_id=tree_id,
            max_cache_size=max_cache_size
        )

        # 恢复标签
        node._tags = set(data.get('tags', []))

        # 恢复生命周期
        node.created_at = datetime.fromisoformat(data['created_at'])
        if data.get('deleted_at'):
            node.deleted_at = datetime.fromisoformat(data['deleted_at'])
        node.is_active = data.get('is_active', True)

        # 恢复Timeline数据
        for dim, tl_data in data.get('timelines', {}).items():
            tl = Timeline.from_dict(
                tl_data,
                storage=storage,
                tree_id=tree_id
            )
            node._timelines[dim] = tl

        # 注意：children关系需要在树重建时设置
        return node

    # ========== 统计信息 ==========

    def get_stats(self) -> Dict[str, Any]:
        """
        获取节点统计信息

        Returns:
            {
                'dimensions': 维度数量,
                'total_points': 总数据点数,
                'cache_size': 当前缓存大小,
                'storage': 是否持久化
            }
        """
        total_points = 0
        cache_size = 0

        for dim, tl in self._timelines.items():
            total_points += len(tl)  # 历史总数
            cache_size += tl.size()   # 当前缓存

        return {
            'node_id': self.node_id,
            'name': self.name,
            'dimensions': len(self._timelines),
            'total_points': total_points,
            'cache_size': cache_size,
            'storage_enabled': self._storage is not None,
            'is_active': self.is_active,
            'created_at': self.created_at,
            'deleted_at': self.deleted_at
        }

    # ========== 特殊方法 ==========

    def __repr__(self) -> str:
        status = "✓" if self.is_active else "✗"
        return f"TreeNode({self.name}, ip={self.ip}, dims={len(self._timelines)})[{status}]"

    def __eq__(self, other) -> bool:
        if not isinstance(other, TreeNode):
            return False
        return self.node_id == other.node_id

    def __hash__(self) -> int:
        return hash(self.node_id)