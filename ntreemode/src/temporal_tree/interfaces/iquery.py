"""
查询接口定义
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime
from .inode import INode


class IQuery(ABC):
    """查询接口 - 定义查询行为"""

    @abstractmethod
    def execute(self) -> Any:
        """
        执行查询

        Returns:
            查询结果
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """
        验证查询是否有效

        Returns:
            是否有效
        """
        pass


class IQueryBuilder(ABC):
    """查询构建器接口"""

    @abstractmethod
    def select(self, *dimensions: str) -> 'IQueryBuilder':
        """
        选择查询的维度

        Args:
            *dimensions: 维度名称列表

        Returns:
            查询构建器自身（用于链式调用）
        """
        pass

    @abstractmethod
    def from_tree(self, tree_id: str) -> 'IQueryBuilder':
        """
        指定查询的树

        Args:
            tree_id: 树ID

        Returns:
            查询构建器自身
        """
        pass

    @abstractmethod
    def where(self, condition: Dict[str, Any]) -> 'IQueryBuilder':
        """
        添加查询条件

        Args:
            condition: 条件字典

        Returns:
            查询构建器自身
        """
        pass

    @abstractmethod
    def time_range(self, start: datetime, end: datetime) -> 'IQueryBuilder':
        """
        设置时间范围

        Args:
            start: 开始时间
            end: 结束时间

        Returns:
            查询构建器自身
        """
        pass

    @abstractmethod
    def at_time(self, timestamp: datetime) -> 'IQueryBuilder':
        """
        设置特定时间点

        Args:
            timestamp: 时间戳

        Returns:
            查询构建器自身
        """
        pass

    @abstractmethod
    def group_by(self, *fields: str) -> 'IQueryBuilder':
        """
        设置分组字段

        Args:
            *fields: 分组字段列表

        Returns:
            查询构建器自身
        """
        pass

    @abstractmethod
    def order_by(self, field: str, descending: bool = False) -> 'IQueryBuilder':
        """
        设置排序

        Args:
            field: 排序字段
            descending: 是否降序

        Returns:
            查询构建器自身
        """
        pass

    @abstractmethod
    def limit(self, count: int) -> 'IQueryBuilder':
        """
        限制结果数量

        Args:
            count: 最大结果数

        Returns:
            查询构建器自身
        """
        pass

    @abstractmethod
    def build(self) -> IQuery:
        """
        构建查询对象

        Returns:
            查询对象
        """
        pass