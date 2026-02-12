"""
IP地址类 - 表示和管理增量编码的IP地址
"""
from typing import List, Optional
from ...exceptions import InvalidIPFormatError


class IPAddress:
    """IP地址类，表示增量编码系统中的地址"""

    def __init__(self, ip_string: str, max_segments: int = 10, max_value: int = 255):
        """
        初始化IP地址

        Args:
            ip_string: IP地址字符串，如 "10.0.0.1"
            max_segments: 最大段数（树的最大深度）
            max_value: 每段最大值

        Raises:
            InvalidIPFormatError: IP格式无效
        """
        self._ip_string = ip_string
        self._max_segments = max_segments
        self._max_value = max_value
        self._segments = self._parse_ip(ip_string)

    def _parse_ip(self, ip_string: str) -> List[int]:
        """
        解析IP字符串为段列表

        Args:
            ip_string: IP地址字符串

        Returns:
            段列表

        Raises:
            InvalidIPFormatError: 格式无效时抛出
        """
        if not ip_string:
            raise InvalidIPFormatError(ip_address=ip_string, reason="IP地址不能为空")

        parts = ip_string.split('.')

        # 验证段数
        if len(parts) > self._max_segments:
            raise InvalidIPFormatError(
                ip_address=ip_string,
                reason=f"段数超过限制: {len(parts)} > {self._max_segments}"
            )

        segments = []
        for i, part in enumerate(parts):
            # 验证是否为数字
            if not part.isdigit():
                raise InvalidIPFormatError(
                    ip_address=ip_string,
                    reason=f"第{i + 1}段不是数字: {part}"
                )

            value = int(part)

            # 验证值范围
            if value < 0 or value > self._max_value:
                raise InvalidIPFormatError(
                    ip_address=ip_string,
                    reason=f"第{i + 1}段值超出范围: {value} (允许: 0-{self._max_value})"
                )

            segments.append(value)

        return segments

    @property
    def segments(self) -> List[int]:
        """获取段列表"""
        return self._segments.copy()

    @property
    def level(self) -> int:
        """获取层级（段数-1）"""
        return len(self._segments) - 1

    @property
    def string(self) -> str:
        """获取字符串表示"""
        return '.'.join(str(segment) for segment in self._segments)

    def get_parent_ip(self) -> Optional['IPAddress']:
        """
        获取父节点IP

        Returns:
            父节点IP，如果是根节点则返回None
        """
        if len(self._segments) <= 1:
            return None

        parent_segments = self._segments[:-1]
        return IPAddress('.'.join(str(s) for s in parent_segments))

    def get_child_ip(self, child_index: int) -> 'IPAddress':
        """
        获取子节点IP

        Args:
            child_index: 子节点索引（从0开始）

        Returns:
            子节点IP

        Raises:
            ValueError: 子节点索引无效
        """
        if child_index < 0 or child_index > self._max_value:
            raise ValueError(f"子节点索引无效: {child_index} (允许: 0-{self._max_value})")

        child_segments = self._segments + [child_index]
        return IPAddress('.'.join(str(s) for s in child_segments))

    def get_sibling_ip(self, offset: int = 1) -> Optional['IPAddress']:
        """
        获取兄弟节点IP

        Args:
            offset: 偏移量，1表示下一个兄弟，-1表示上一个兄弟

        Returns:
            兄弟节点IP，如果不存在返回None
        """
        if len(self._segments) == 0:
            return None

        last_segment = self._segments[-1]
        new_last = last_segment + offset

        # 检查是否超出范围
        if new_last < 0 or new_last > self._max_value:
            return None

        sibling_segments = self._segments[:-1] + [new_last]
        return IPAddress('.'.join(str(s) for s in sibling_segments))

    def is_root(self) -> bool:
        """是否是根节点IP"""
        return len(self._segments) == 1

    def is_descendant_of(self, other: 'IPAddress') -> bool:
        """
        判断是否是另一个IP的后代

        Args:
            other: 另一个IP地址

        Returns:
            如果是后代返回True
        """
        if len(self._segments) <= len(other.segments):
            return False

        # 检查前缀是否相同
        for i in range(len(other.segments)):
            if self._segments[i] != other.segments[i]:
                return False

        return True

    def is_ancestor_of(self, other: 'IPAddress') -> bool:
        """
        判断是否是另一个IP的祖先

        Args:
            other: 另一个IP地址

        Returns:
            如果是祖先返回True
        """
        return other.is_descendant_of(self)

    def __str__(self) -> str:
        return self.string

    def __repr__(self) -> str:
        return f"IPAddress('{self.string}')"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IPAddress):
            return False
        return self._segments == other._segments

    def __lt__(self, other: 'IPAddress') -> bool:
        """比较两个IP地址（用于排序）"""
        # 逐段比较
        for i in range(min(len(self._segments), len(other.segments))):
            if self._segments[i] < other.segments[i]:
                return True
            elif self._segments[i] > other.segments[i]:
                return False

        # 如果前缀相同，短的更小
        return len(self._segments) < len(other.segments)

    def __hash__(self) -> int:
        return hash(tuple(self._segments))