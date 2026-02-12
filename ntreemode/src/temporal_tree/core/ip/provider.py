# -*- coding: utf-8 -*-
"""
IP提供者实现 - 管理增量编码分配
"""
from typing import Dict, List, Optional
from ...interfaces import IIPProvider
from ...exceptions import IPAllocationError, InvalidIPFormatError
from .address import IPAddress


class IncrementalIPProvider(IIPProvider):
    """
    增量IP提供者

    使用增量编码系统分配IP地址，例如：
    根节点: 10.0.0.0
    子节点: 10.0.0.0.1, 10.0.0.0.2, ...
    孙节点: 10.0.0.0.1.1, 10.0.0.0.1.2, ...
    """

    def __init__(self, base_ip: str = "10.0.0.0",
                 max_depth: int = 10, max_children_per_node: int = 100):
        """
        初始化IP提供者

        Args:
            base_ip: 基础IP地址
            max_depth: 树的最大深度
            max_children_per_node: 每个节点最大子节点数
        """
        self._base_ip = IPAddress(base_ip)
        self._max_depth = max_depth
        self._max_children_per_node = max_children_per_node

        # 记录已分配的IP和它们的子节点计数
        self._allocated_ips: Dict[str, IPAddress] = {}
        self._child_counts: Dict[str, int] = {}

        # 分配根IP
        self._allocate_ip(self._base_ip.string)

    def _allocate_ip(self, ip_string: str) -> IPAddress:
        """
        内部方法：分配IP地址

        Args:
            ip_string: IP地址字符串

        Returns:
            IPAddress对象
        """
        ip = IPAddress(ip_string)
        self._allocated_ips[ip.string] = ip
        self._child_counts[ip.string] = 0
        return ip

    def allocate_root_ip(self) -> str:
        """分配根节点IP地址"""
        # 根IP已经在初始化时分配，这里返回
        return self._base_ip.string

    def allocate_child_ip(self, parent_ip: str) -> str:
        """为父节点分配子节点IP地址"""
        # 验证父IP
        if parent_ip not in self._allocated_ips:
            raise IPAllocationError(
                parent_ip=parent_ip,
                reason="父节点IP未分配"
            )

        parent = self._allocated_ips[parent_ip]

        # 检查深度限制
        if parent.level >= self._max_depth - 1:
            raise IPAllocationError(
                parent_ip=parent_ip,
                reason=f"达到最大深度限制: {self._max_depth}"
            )

        # 检查子节点数限制
        child_count = self._child_counts[parent_ip]
        if child_count >= self._max_children_per_node:
            raise IPAllocationError(
                parent_ip=parent_ip,
                reason=f"达到子节点数限制: {self._max_children_per_node}"
            )

        # 分配新的子节点IP
        try:
            child_ip = parent.get_child_ip(child_count)
        except ValueError as e:
            raise IPAllocationError(
                parent_ip=parent_ip,
                reason=str(e)
            )

        # 检查是否已分配
        if child_ip.string in self._allocated_ips:
            raise IPAllocationError(
                parent_ip=parent_ip,
                reason=f"IP地址已分配: {child_ip.string}"
            )

        # 分配IP
        self._allocate_ip(child_ip.string)

        # 更新父节点的子节点计数
        self._child_counts[parent_ip] = child_count + 1

        return child_ip.string

    def get_sibling_ip(self, current_ip: str, offset: int = 1) -> Optional[str]:
        """获取兄弟节点IP地址"""
        if current_ip not in self._allocated_ips:
            return None

        current = self._allocated_ips[current_ip]
        sibling = current.get_sibling_ip(offset)

        if sibling is None:
            return None

        # 检查兄弟IP是否已分配
        if sibling.string in self._allocated_ips:
            return sibling.string

        return None

    def get_parent_ip(self, child_ip: str) -> Optional[str]:
        """获取父节点IP地址"""
        if child_ip not in self._allocated_ips:
            return None

        child = self._allocated_ips[child_ip]
        parent = child.get_parent_ip()

        if parent is None:
            return None

        return parent.string if parent.string in self._allocated_ips else None

    def get_ip_level(self, ip_address: str) -> int:
        """获取IP地址的层级"""
        if ip_address not in self._allocated_ips:
            raise InvalidIPFormatError(
                ip_address=ip_address,
                reason="IP地址未分配"
            )

        return self._allocated_ips[ip_address].level

    def validate_ip(self, ip_address: str) -> bool:
        """验证IP地址格式是否有效"""
        try:
            ip = IPAddress(ip_address)

            # 检查是否以基础IP开头
            if not ip.is_descendant_of(self._base_ip) and ip != self._base_ip:
                return False

            # 检查深度
            if ip.level >= self._max_depth:
                return False

            return True

        except InvalidIPFormatError:
            return False

    def compare_ips(self, ip1: str, ip2: str) -> int:
        """比较两个IP地址"""
        try:
            ip_obj1 = IPAddress(ip1)
            ip_obj2 = IPAddress(ip2)

            if ip_obj1 < ip_obj2:
                return -1
            elif ip_obj1 == ip_obj2:
                return 0
            else:
                return 1

        except InvalidIPFormatError:
            raise ValueError(f"无效的IP地址: {ip1} 或 {ip2}")

    def get_ip_segments(self, ip_address: str) -> List[int]:
        """将IP地址分解为段列表"""
        ip = IPAddress(ip_address)
        return ip.segments

    def format_ip(self, segments: List[int]) -> str:
        """将段列表格式化为IP地址"""
        return '.'.join(str(segment) for segment in segments)

    def get_max_children_per_node(self) -> int:
        """获取每个节点最多允许的子节点数"""
        return self._max_children_per_node

    def get_max_depth(self) -> int:
        """获取树的最大深度"""
        return self._max_depth

    def get_allocated_ips(self) -> List[str]:
        """获取所有已分配的IP地址"""
        return list(self._allocated_ips.keys())

    def get_child_count(self, parent_ip: str) -> int:
        """获取父节点的子节点数"""
        return self._child_counts.get(parent_ip, 0)

    def is_ip_allocated(self, ip_address: str) -> bool:
        """检查IP地址是否已分配"""
        return ip_address in self._allocated_ips

    def reset(self) -> None:
        """重置IP分配器（清空所有分配）"""
        self._allocated_ips.clear()
        self._child_counts.clear()
        # 重新分配根IP
        self._allocate_ip(self._base_ip.string)