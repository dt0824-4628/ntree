"""
存储适配器接口
定义统一的存储操作接口
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from .exceptions import DataStoreError
from temporal_tree.exceptions import TreeNotFoundError, NodeNotFoundError

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

try:
    from temporal_tree.exceptions import (
        DataStoreError, TreeNotFoundError, NodeNotFoundError
    )
except ImportError:
    # 备用导入
    from ...exceptions import (
        DataStoreError, TreeNotFoundError, NodeNotFoundError
    )

class DataStoreAdapter(ABC):
    """数据存储适配器抽象基类"""

    @abstractmethod
    def save_tree(self, tree_data: Dict[str, Any]) -> bool:
        """保存树数据"""
        pass

    @abstractmethod
    def load_tree(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """加载树数据"""
        pass

    @abstractmethod
    def delete_tree(self, tree_id: str) -> bool:
        """删除树"""
        pass

    @abstractmethod
    def save_time_point(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            timestamp: datetime,
            value: Any,
            quality: int = 1,
            unit: Optional[str] = None
    ) -> None:
        """
        保存单个时间点数据（三维坐标系中的一个点）

        Args:
            tree_id: 树ID（命名空间）
            node_id: 节点ID（IP地址标识）
            dimension: 维度名称（如 'meter_gas', 'pressure'）
            timestamp: 时间戳
            value: 数值
            quality: 质量码（0=无效,1=正常,2=估算,3=缺失）
            unit: 单位（可选，用于显示）

        说明：
            - 每个点都是独立的，维度名就是字符串，不需要预定义
            - 同一个(节点,维度,时间)只能有一个值，后写入覆盖
        """
        pass

    @abstractmethod
    def get_time_points(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None,
            limit: Optional[int] = None
    ) -> List[Tuple[datetime, Any, Dict]]:
        """
        获取时间范围内的所有时间点

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            start_time: 开始时间（含），None表示不限制开始
            end_time: 结束时间（含），None表示不限制结束
            limit: 最多返回条数，None表示不限制

        Returns:
            List of (timestamp, value, metadata)
            metadata包含quality, unit等信息
        """
        pass

    @abstractmethod
    def get_latest_time_point(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            before_time: Optional[datetime] = None
    ) -> Optional[Tuple[datetime, Any, Dict]]:
        """
        获取最新的时间点

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            before_time: 在此时间之前的最新的点，None表示所有时间

        Returns:
            (timestamp, value, metadata) 或 None
        """
        pass

    @abstractmethod
    def delete_time_points(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            before_time: Optional[datetime] = None
    ) -> int:
        """
        删除时间点（软删除/硬删除由具体实现决定）

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            before_time: 删除此时间之前的所有点，None表示删除所有

        Returns:
            删除的记录数
        """
        pass

    @abstractmethod
    def get_dimensions(
            self,
            tree_id: str,
            node_id: Optional[str] = None
    ) -> List[str]:
        """
        获取所有出现过维度名称

        Args:
            tree_id: 树ID
            node_id: 节点ID，如果提供则只返回该节点的维度

        Returns:
            维度名称列表，按首次出现时间排序

        说明：
            - 维度是"发现"的，不是"定义"的
            - 只要有一个时间点，维度就会被发现
        """
        pass

    @abstractmethod
    def get_time_range(
            self,
            tree_id: str,
            node_id: str,
            dimension: str
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        获取某个维度数据的时间范围

        Returns:
            (min_time, max_time)，如果没有数据返回(None, None)
        """
        pass


class TimePointMetadata:
    """时间点元数据，用于get_time_points的返回"""

    def __init__(
            self,
            quality: int = 1,
            unit: Optional[str] = None,
            created_at: Optional[datetime] = None
    ):
        self.quality = quality
        self.unit = unit
        self.created_at = created_at or datetime.now()

    def to_dict(self) -> Dict:
        return {
            'quality': self.quality,
            'unit': self.unit,
            'created_at': self.created_at.isoformat()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'TimePointMetadata':
        return cls(
            quality=data.get('quality', 1),
            unit=data.get('unit'),
            created_at=datetime.fromisoformat(data['created_at']) if 'created_at' in data else None
        )

    @abstractmethod
    def list_trees(self) -> List[Dict[str, Any]]:
        """列出所有树"""
        pass

    @abstractmethod
    def save_node(self, tree_id: str, node_data: Dict[str, Any]) -> bool:
        """保存节点数据"""
        pass

    @abstractmethod
    def load_node(self, tree_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """加载节点数据"""
        pass

    @abstractmethod
    def load_all_nodes(self, tree_id: str) -> List[Dict[str, Any]]:
        """加载树的所有节点"""
        pass

    @abstractmethod
    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """删除节点"""
        pass

    @abstractmethod
    def save_node_data(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            value: Any,
            timestamp: datetime
    ) -> bool:
        """保存节点维度数据"""
        pass

    @abstractmethod
    def load_node_data(
            self,
            tree_id: str,
            node_id: str,
            dimension: Optional[str] = None,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """加载节点维度数据"""
        pass

    @abstractmethod
    def exists_tree(self, tree_id: str) -> bool:
        """检查树是否存在"""
        pass

    @abstractmethod
    def exists_node(self, tree_id: str, node_id: str) -> bool:
        """检查节点是否存在"""
        pass

    @abstractmethod
    def get_tree_stats(self, tree_id: str) -> Dict[str, Any]:
        """获取树统计信息"""
        pass

    @abstractmethod
    def save_timeline(self, timeline_data: Dict[str, Any]) -> bool:
        """保存时间线"""
        pass

    @abstractmethod
    def load_timeline(self, timeline_id: str) -> Optional[Dict[str, Any]]:
        """加载时间线"""
        pass

    @abstractmethod
    def delete_timeline(self, timeline_id: str) -> bool:
        """删除时间线"""
        pass

    @abstractmethod
    def list_timelines(self, object_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """列出时间线（可按对象ID过滤）"""
        pass

    @abstractmethod
    def save_time_point(self, timeline_id: str, time_point: Dict[str, Any]) -> bool:
        """保存时间点（追加）"""
        pass

    @abstractmethod
    def load_time_points(
            self,
            timeline_id: str,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """加载时间点（支持时间范围）"""
        pass
    @abstractmethod
    def close(self):
        """关闭存储连接"""
        pass

    @abstractmethod
    def clear(self):
        """清空所有数据（测试用）"""
        pass


class StorageContext:
    """存储上下文管理器"""

    def __init__(self, adapter: DataStoreAdapter):
        self.adapter = adapter

    def __enter__(self):
        return self.adapter

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.adapter.close()