"""
节点接口定义
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime


class INode(ABC):
    """节点接口 - 定义树节点的基本行为"""

    @property
    @abstractmethod
    def node_id(self) -> str:
        """节点唯一标识"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """节点名称"""
        pass

    @property
    @abstractmethod
    def level(self) -> int:
        """节点层级（0为根节点）"""
        pass

    @property
    @abstractmethod
    def ip_address(self) -> str:
        """节点IP地址（增量编码）"""
        pass

    @property
    @abstractmethod
    def parent(self) -> Optional['INode']:
        """父节点"""
        pass

    @parent.setter
    @abstractmethod
    def parent(self, node: Optional['INode']) -> None:
        """设置父节点"""
        pass

    @property
    @abstractmethod
    def children(self) -> List['INode']:
        """子节点列表"""
        pass

    @abstractmethod
    def add_child(self, child: 'INode') -> 'INode':
        """添加子节点"""
        pass

    @abstractmethod
    def remove_child(self, child_id: str) -> bool:
        """移除子节点"""
        pass

    @abstractmethod
    def get_child_by_ip(self, ip_address: str) -> Optional['INode']:
        """根据IP地址获取子节点"""
        pass

    @abstractmethod
    def get_child_by_name(self, name: str) -> Optional['INode']:
        """根据名称获取子节点"""
        pass

    @abstractmethod
    def get_data(self, dimension: str, timestamp: Optional[datetime] = None) -> Any:
        """
        获取指定维度的数据

        Args:
            dimension: 维度名称
            timestamp: 时间戳，None表示最新数据

        Returns:
            维度数据
        """
        pass

    @abstractmethod
    def set_data(self, dimension: str, value: Any,
                 timestamp: Optional[datetime] = None) -> None:
        """
        设置维度数据

        Args:
            dimension: 维度名称
            value: 数据值
            timestamp: 时间戳，None表示当前时间
        """
        pass

    @abstractmethod
    def get_all_dimensions(self) -> List[str]:
        """获取所有可用的维度名称"""
        pass

    @abstractmethod
    def has_dimension(self, dimension: str) -> bool:
        """检查是否包含指定维度"""
        pass

    @abstractmethod
    def get_descendants(self) -> List['INode']:
        """获取所有后代节点"""
        pass

    @abstractmethod
    def get_ancestors(self) -> List['INode']:
        """获取所有祖先节点（从父节点到根节点）"""
        pass

    @abstractmethod
    def to_dict(self, include_children: bool = False,
                include_data: bool = False) -> Dict[str, Any]:
        """
        转换为字典

        Args:
            include_children: 是否包含子节点信息
            include_data: 是否包含维度数据

        Returns:
            节点字典表示
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """验证节点数据是否有效"""
        pass