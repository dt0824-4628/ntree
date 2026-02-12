"""
维度接口定义
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from datetime import datetime


class IDimension(ABC):
    """维度接口 - 定义数据维度的行为"""

    @property
    @abstractmethod
    def name(self) -> str:
        """维度名称"""
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """显示名称"""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """维度描述"""
        pass

    @property
    @abstractmethod
    def data_type(self) -> type:
        """数据类型"""
        pass

    @property
    @abstractmethod
    def unit(self) -> str:
        """数据单位"""
        pass

    @property
    @abstractmethod
    def is_calculated(self) -> bool:
        """是否为计算维度"""
        pass

    @abstractmethod
    def validate(self, value: Any) -> bool:
        """
        验证数据值

        Args:
            value: 待验证的值

        Returns:
            是否有效
        """
        pass

    @abstractmethod
    def format(self, value: Any) -> str:
        """
        格式化数据值

        Args:
            value: 数据值

        Returns:
            格式化后的字符串
        """
        pass

    @abstractmethod
    def calculate(self, node: Any, timestamp: Optional[datetime] = None) -> Any:
        """
        计算维度值（仅对计算维度有效）

        Args:
            node: 节点对象
            timestamp: 时间戳

        Returns:
            计算后的值

        Raises:
            NotImplementedError: 如果不是计算维度
        """
        pass

    @abstractmethod
    def get_default_value(self) -> Any:
        """
        获取默认值

        Returns:
            默认值
        """
        pass

    @abstractmethod
    def get_valid_range(self) -> Optional[Dict[str, Any]]:
        """
        获取有效范围

        Returns:
            包含min和max的字典，或None表示无限制
        """
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        获取维度元数据

        Returns:
            元数据字典
        """
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典

        Returns:
            字典表示
        """
        pass