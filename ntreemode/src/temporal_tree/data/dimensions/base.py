"""
维度基类
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from datetime import datetime

from ...interfaces import IDimension
from ...exceptions import DimensionValidationError


class BaseDimension(IDimension, ABC):
    """维度基类，提供通用实现"""

    def __init__(self,
                 name: str,
                 display_name: str,
                 description: str,
                 data_type: type,
                 unit: str,
                 is_calculated: bool = False):
        """
        初始化维度

        Args:
            name: 维度名称（英文标识）
            display_name: 显示名称（中文）
            description: 维度描述
            data_type: 数据类型
            unit: 数据单位
            is_calculated: 是否为计算维度
        """
        self._name = name
        self._display_name = display_name
        self._description = description
        self._data_type = data_type
        self._unit = unit
        self._is_calculated = is_calculated
        self._metadata = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def display_name(self) -> str:
        return self._display_name

    @property
    def description(self) -> str:
        return self._description

    @property
    def data_type(self) -> type:
        return self._data_type

    @property
    def unit(self) -> str:
        return self._unit

    @property
    def is_calculated(self) -> bool:
        return self._is_calculated

    def validate(self, value: Any) -> bool:
        """验证数据值"""
        try:
            # 检查类型
            if not isinstance(value, self._data_type):
                # 尝试类型转换
                if self._data_type == float:
                    float(value)
                elif self._data_type == int:
                    int(value)
                else:
                    return False

            # 调用具体的验证逻辑
            return self._validate_impl(value)

        except (ValueError, TypeError):
            return False

    @abstractmethod
    def _validate_impl(self, value: Any) -> bool:
        """具体的验证逻辑（由子类实现）"""
        pass

    def format(self, value: Any) -> str:
        """格式化数据值"""
        if value is None:
            return "N/A"

        try:
            if self._data_type == float:
                return f"{float(value):.2f} {self._unit}"
            elif self._data_type == int:
                return f"{int(value)} {self._unit}"
            else:
                return f"{value} {self._unit}"
        except:
            return str(value)

    def calculate(self, node: Any, timestamp: Optional[datetime] = None) -> Any:
        """计算维度值"""
        if not self._is_calculated:
            raise NotImplementedError(f"维度 '{self.name}' 不是计算维度")

        return self._calculate_impl(node, timestamp)

    def _calculate_impl(self, node: Any, timestamp: Optional[datetime] = None) -> Any:
        """具体的计算逻辑（由子类实现）"""
        raise NotImplementedError

    def get_default_value(self) -> Any:
        """获取默认值"""
        if self._data_type == float:
            return 0.0
        elif self._data_type == int:
            return 0
        elif self._data_type == str:
            return ""
        elif self._data_type == bool:
            return False
        else:
            return None

    def get_valid_range(self) -> Optional[Dict[str, Any]]:
        """获取有效范围"""
        return None

    def get_metadata(self) -> Dict[str, Any]:
        """获取维度元数据"""
        return {
            "name": self._name,
            "display_name": self._display_name,
            "description": self._description,
            "data_type": self._data_type.__name__,
            "unit": self._unit,
            "is_calculated": self._is_calculated,
            "default_value": self.get_default_value(),
            "valid_range": self.get_valid_range(),
            **self._metadata
        }

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return self.get_metadata()

    def __str__(self) -> str:
        return f"{self._display_name} ({self._name})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self._name}')"