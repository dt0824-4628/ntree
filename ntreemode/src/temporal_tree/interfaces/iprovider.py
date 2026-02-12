"""
IP地址提供者接口
"""
from abc import ABC, abstractmethod
from typing import Optional, List


class IIPProvider(ABC):
    """IP地址分配器接口 - 管理增量编码系统"""

    @abstractmethod
    def allocate_root_ip(self) -> str:
        """
        分配根节点IP地址

        Returns:
            根节点IP地址，如 "10.0.0.0"
        """
        pass

    @abstractmethod
    def allocate_child_ip(self, parent_ip: str) -> str:
        """
        为父节点分配子节点IP地址

        Args:
            parent_ip: 父节点IP地址

        Returns:
            子节点IP地址，如 "10.0.0.0.1"

        Raises:
            ValueError: 如果父节点IP无效或无法分配
        """
        pass

    @abstractmethod
    def get_sibling_ip(self, current_ip: str, offset: int = 1) -> Optional[str]:
        """
        获取兄弟节点IP地址

        Args:
            current_ip: 当前节点IP
            offset: 偏移量，1表示下一个兄弟，-1表示上一个兄弟

        Returns:
            兄弟节点IP地址，如果不存在返回None
        """
        pass

    @abstractmethod
    def get_parent_ip(self, child_ip: str) -> Optional[str]:
        """
        获取父节点IP地址

        Args:
            child_ip: 子节点IP

        Returns:
            父节点IP地址，如果是根节点返回None
        """
        pass

    @abstractmethod
    def get_ip_level(self, ip_address: str) -> int:
        """
        获取IP地址的层级

        Args:
            ip_address: IP地址

        Returns:
            层级数（0为根）
        """
        pass

    @abstractmethod
    def validate_ip(self, ip_address: str) -> bool:
        """
        验证IP地址格式是否有效

        Args:
            ip_address: 待验证的IP

        Returns:
            是否有效
        """
        pass

    @abstractmethod
    def compare_ips(self, ip1: str, ip2: str) -> int:
        """
        比较两个IP地址

        Args:
            ip1: 第一个IP
            ip2: 第二个IP

        Returns:
            -1: ip1 < ip2
            0: ip1 == ip2
            1: ip1 > ip2
        """
        pass

    @abstractmethod
    def get_ip_segments(self, ip_address: str) -> List[int]:
        """
        将IP地址分解为段列表

        Args:
            ip_address: IP地址

        Returns:
            段列表，如 "10.0.0.1" -> [10, 0, 0, 1]
        """
        pass

    @abstractmethod
    def format_ip(self, segments: List[int]) -> str:
        """
        将段列表格式化为IP地址

        Args:
            segments: 段列表

        Returns:
            格式化后的IP地址
        """
        pass

    @abstractmethod
    def get_max_children_per_node(self) -> int:
        """
        获取每个节点最多允许的子节点数

        Returns:
            最大子节点数
        """
        pass

    @abstractmethod
    def get_max_depth(self) -> int:
        """
        获取树的最大深度

        Returns:
            最大深度
        """
        pass