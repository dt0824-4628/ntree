"""
存储适配器接口
定义统一的存储操作接口
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
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