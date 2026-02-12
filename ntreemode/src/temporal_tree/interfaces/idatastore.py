"""
数据存储接口
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime
from .inode import INode


class IDataStore(ABC):
    """数据存储接口 - 抽象数据持久化层"""

    @abstractmethod
    def save_tree(self, tree_id: str, root_node: INode) -> bool:
        """
        保存整棵树

        Args:
            tree_id: 树ID
            root_node: 根节点

        Returns:
            是否保存成功
        """
        pass

    @abstractmethod
    def load_tree(self, tree_id: str) -> Optional[INode]:
        """
        加载树

        Args:
            tree_id: 树ID

        Returns:
            根节点，如果不存在返回None
        """
        pass

    @abstractmethod
    def save_node(self, tree_id: str, node: INode) -> bool:
        """
        保存单个节点

        Args:
            tree_id: 树ID
            node: 节点

        Returns:
            是否保存成功
        """
        pass

    @abstractmethod
    def load_node(self, tree_id: str, node_id: str) -> Optional[INode]:
        """
        加载单个节点

        Args:
            tree_id: 树ID
            node_id: 节点ID

        Returns:
            节点，如果不存在返回None
        """
        pass

    @abstractmethod
    def delete_tree(self, tree_id: str) -> bool:
        """
        删除树

        Args:
            tree_id: 树ID

        Returns:
            是否删除成功
        """
        pass

    @abstractmethod
    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """
        删除节点

        Args:
            tree_id: 树ID
            node_id: 节点ID

        Returns:
            是否删除成功
        """
        pass

    @abstractmethod
    def save_node_data(self, tree_id: str, node_id: str,
                       dimension: str, value: Any,
                       timestamp: datetime) -> bool:
        """
        保存节点维度数据

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            value: 数据值
            timestamp: 时间戳

        Returns:
            是否保存成功
        """
        pass

    @abstractmethod
    def load_node_data(self, tree_id: str, node_id: str,
                       dimension: str,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None) -> Dict[datetime, Any]:
        """
        加载节点维度数据

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            start_time: 起始时间
            end_time: 结束时间

        Returns:
            时间戳到数据的映射
        """
        pass

    @abstractmethod
    def list_trees(self) -> List[str]:
        """
        列出所有树ID

        Returns:
            树ID列表
        """
        pass

    @abstractmethod
    def tree_exists(self, tree_id: str) -> bool:
        """
        检查树是否存在

        Args:
            tree_id: 树ID

        Returns:
            是否存在
        """
        pass

    @abstractmethod
    def node_exists(self, tree_id: str, node_id: str) -> bool:
        """
        检查节点是否存在

        Args:
            tree_id: 树ID
            node_id: 节点ID

        Returns:
            是否存在
        """
        pass

    @abstractmethod
    def get_tree_stats(self, tree_id: str) -> Dict[str, Any]:
        """
        获取树统计信息

        Args:
            tree_id: 树ID

        Returns:
            统计信息字典
        """
        pass

    @abstractmethod
    def backup(self, backup_path: str) -> bool:
        """
        备份数据

        Args:
            backup_path: 备份路径

        Returns:
            是否备份成功
        """
        pass

    @abstractmethod
    def restore(self, backup_path: str) -> bool:
        """
        恢复数据

        Args:
            backup_path: 备份路径

        Returns:
            是否恢复成功
        """
        pass