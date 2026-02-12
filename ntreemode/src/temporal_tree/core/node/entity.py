"""
节点实体实现
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from uuid import uuid4
import json

from ...interfaces import INode
from ...exceptions import NodeError, ValidationError
from ..ip import IPAddress


class TreeNode(INode):
    """
    树节点实体
    实现INode接口，表示燃气输差分析树中的一个节点
    """

    def __init__(self,
                 name: str,
                 ip_address: str,
                 level: int = 0,
                 node_id: Optional[str] = None,
                 parent: Optional['TreeNode'] = None,
                 metadata: Optional[Dict[str, Any]] = None):
        """
        初始化节点

        Args:
            name: 节点名称
            ip_address: 节点IP地址（增量编码）
            level: 节点层级（0为根节点）
            node_id: 节点唯一标识，为None时自动生成
            parent: 父节点
            metadata: 节点元数据
        """
        self._id = node_id or f"node_{uuid4().hex[:8]}"
        self._name = name
        self._level = level
        self._parent = parent

        # 验证并创建IP地址对象
        try:
            self._ip_address_obj = IPAddress(ip_address)
        except Exception as e:
            raise ValidationError(
                message=f"无效的IP地址: {ip_address}",
                field="ip_address",
                value=ip_address,
                reason=str(e)
            )

        # 子节点管理
        self._children: List['TreeNode'] = []

        # 维度数据存储：{维度名: {时间戳: 值}}
        self._data_store: Dict[str, Dict[datetime, Any]] = {}

        # 节点元数据
        self._metadata = metadata or {}
        self._metadata.update({
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

        # 节点标签
        self._tags: List[str] = []

        # 维度计算器：{维度名: 计算函数}
        self._calculators: Dict[str, callable] = {}

    @property
    def node_id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str):
        """设置节点名称"""
        if not value or not isinstance(value, str):
            raise ValidationError(
                message="节点名称不能为空",
                field="name",
                value=value,
                reason="invalid_name"
            )
        self._name = value
        self._metadata["updated_at"] = datetime.now()

    @property
    def level(self) -> int:
        return self._level

    @property
    def ip_address(self) -> str:
        return str(self._ip_address_obj)

    @property
    def parent(self) -> Optional['TreeNode']:
        return self._parent

    @parent.setter
    def parent(self, node: Optional['TreeNode']) -> None:
        """设置父节点"""
        if node is self:
            raise NodeError("节点不能作为自己的父节点")

        # 断开原来的父子关系
        if self._parent and self in self._parent._children:
            self._parent._children.remove(self)

        # 建立新的父子关系
        self._parent = node
        if node and self not in node._children:
            node._children.append(self)

        # 更新层级
        if node:
            self._level = node.level + 1
        else:
            self._level = 0

        self._metadata["updated_at"] = datetime.now()

    @property
    def children(self) -> List['TreeNode']:
        return self._children.copy()

    def add_child(self, child: 'TreeNode') -> 'TreeNode':
        """添加子节点"""
        if child in self._children:
            raise NodeError(f"子节点已存在: {child.name}")

        # 设置子节点的父节点
        child.parent = self

        # 确保子节点被添加到列表
        if child not in self._children:
            self._children.append(child)

        self._metadata["updated_at"] = datetime.now()
        return child

    def remove_child(self, child_id: str) -> bool:
        """移除子节点"""
        for i, child in enumerate(self._children):
            if child.node_id == child_id:
                # 断开父子关系
                child._parent = None
                child._level = 0

                # 从列表中移除
                self._children.pop(i)

                self._metadata["updated_at"] = datetime.now()
                return True

        return False

    def get_child_by_ip(self, ip_address: str) -> Optional['TreeNode']:
        """根据IP地址获取子节点"""
        for child in self._children:
            if child.ip_address == ip_address:
                return child
        return None

    def get_child_by_name(self, name: str) -> Optional['TreeNode']:
        """根据名称获取子节点"""
        for child in self._children:
            if child.name == name:
                return child
        return None

    def get_data(self, dimension: str, timestamp: Optional[datetime] = None) -> Any:
        """
        获取指定维度的数据

        Args:
            dimension: 维度名称
            timestamp: 时间戳，None表示最新数据

        Returns:
            维度数据，如果不存在返回None
        """
        # 首先检查是否有计算器
        if dimension in self._calculators:
            return self._calculators[dimension](self, timestamp)

        # 检查数据存储
        if dimension not in self._data_store:
            return None

        dimension_data = self._data_store[dimension]

        if not dimension_data:
            return None

        if timestamp is None:
            # 返回最新数据
            latest_time = max(dimension_data.keys())
            return dimension_data[latest_time]
        else:
            # 返回指定时间数据
            # 如果没有精确匹配，返回最接近的时间点数据
            if timestamp in dimension_data:
                return dimension_data[timestamp]

            # 寻找最接近的时间点
            closest_time = None
            min_diff = None

            for data_time in dimension_data.keys():
                diff = abs((data_time - timestamp).total_seconds())
                if min_diff is None or diff < min_diff:
                    min_diff = diff
                    closest_time = data_time

            if closest_time and min_diff < 3600:  # 1小时内的容忍度
                return dimension_data[closest_time]

            return None

    def set_data(self, dimension: str, value: Any,
                 timestamp: Optional[datetime] = None) -> None:
        """
        设置维度数据

        Args:
            dimension: 维度名称
            value: 数据值
            timestamp: 时间戳，None表示当前时间
        """
        if timestamp is None:
            timestamp = datetime.now()

        if dimension not in self._data_store:
            self._data_store[dimension] = {}

        self._data_store[dimension][timestamp] = value
        self._metadata["updated_at"] = datetime.now()

    def get_all_dimensions(self) -> List[str]:
        """获取所有可用的维度名称"""
        dimensions = set(self._data_store.keys())
        dimensions.update(self._calculators.keys())
        return sorted(list(dimensions))

    def has_dimension(self, dimension: str) -> bool:
        """检查是否包含指定维度"""
        return dimension in self._data_store or dimension in self._calculators

    def get_descendants(self) -> List['TreeNode']:
        """获取所有后代节点（深度优先遍历）"""
        descendants = []

        def collect(node: 'TreeNode'):
            for child in node.children:
                descendants.append(child)
                collect(child)

        collect(self)
        return descendants

    def get_ancestors(self) -> List['TreeNode']:
        """获取所有祖先节点（从父节点到根节点）"""
        ancestors = []
        current = self.parent

        while current is not None:
            ancestors.append(current)
            current = current.parent

        return ancestors

    def add_dimension_calculator(self, dimension: str, calculator: callable) -> None:
        """
        添加维度计算器

        Args:
            dimension: 维度名称
            calculator: 计算函数，接收(node, timestamp)参数，返回计算值
        """
        self._calculators[dimension] = calculator

    def add_tag(self, tag: str) -> None:
        """添加标签"""
        if tag not in self._tags:
            self._tags.append(tag)

    def remove_tag(self, tag: str) -> bool:
        """移除标签"""
        if tag in self._tags:
            self._tags.remove(tag)
            return True
        return False

    def has_tag(self, tag: str) -> bool:
        """检查是否包含标签"""
        return tag in self._tags

    def get_all_data(self, include_calculated: bool = True) -> Dict[str, Dict[datetime, Any]]:
        """
        获取所有维度数据

        Args:
            include_calculated: 是否包含计算维度

        Returns:
            维度数据字典
        """
        result = self._data_store.copy()

        if include_calculated:
            for dimension in self._calculators.keys():
                if dimension not in result:
                    result[dimension] = {}
                # 计算维度没有历史数据，只有当前值
                result[dimension][datetime.now()] = self.get_data(dimension)

        return result

    def validate(self) -> bool:
        """验证节点数据是否有效"""
        # 验证名称
        if not self._name or not isinstance(self._name, str):
            return False

        # 验证IP地址
        try:
            IPAddress(self.ip_address)
        except:
            return False

        # 验证父子关系一致性
        if self._parent and self not in self._parent.children:
            return False

        # 验证子节点
        for child in self._children:
            if child.parent != self:
                return False

        return True

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
        result = {
            "node_id": self._id,
            "name": self._name,
            "ip_address": self.ip_address,
            "level": self._level,
            "parent_id": self._parent.node_id if self._parent else None,
            "children_count": len(self._children),
            "tags": self._tags.copy(),
            "metadata": self._metadata.copy(),
            "dimensions": self.get_all_dimensions()
        }

        if include_children:
            result["children"] = [
                child.to_dict(include_children=False, include_data=include_data)
                for child in self._children
            ]

        if include_data:
            result["data"] = {
                dimension: {
                    str(timestamp): value
                    for timestamp, value in data.items()
                }
                for dimension, data in self.get_all_data().items()
            }

        return result

    def __str__(self) -> str:
        indent = "  " * self._level
        return f"{indent}{self._name} (IP: {self.ip_address}, ID: {self._id[:8]})"

    def __repr__(self) -> str:
        return f"TreeNode(name='{self._name}', ip='{self.ip_address}', level={self._level})"