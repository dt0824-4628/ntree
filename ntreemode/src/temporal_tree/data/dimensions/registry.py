"""
维度注册表
管理所有可用的数据维度
"""
from typing import Dict, List, Optional, Type, Any
from datetime import datetime  # 添加这行
from ...interfaces import IDimension
from ...exceptions import DimensionNotFoundError, DimensionValidationError

from .gas_standard import StandardGasDimension
from .gas_meter import MeterGasDimension
from .loss_rate import LossRateDimension


class DimensionRegistry:
    """维度注册表，管理所有维度"""

    def __init__(self):
        """初始化维度注册表"""
        self._dimensions: Dict[str, IDimension] = {}
        self._dimension_classes: Dict[str, Type[IDimension]] = {}

        # 注册内置维度
        self._register_builtin_dimensions()

    def _register_builtin_dimensions(self):
        """注册内置维度"""
        self.register(StandardGasDimension())
        self.register(MeterGasDimension())
        self.register(LossRateDimension())

    def register(self, dimension: IDimension) -> None:
        """
        注册维度

        Args:
            dimension: 维度实例

        Raises:
            DimensionValidationError: 维度验证失败
        """
        # 验证维度
        if not isinstance(dimension, IDimension):
            raise DimensionValidationError(
                dimension_name=str(dimension),
                value=dimension,
                reason="必须实现IDimension接口"
            )

        # 检查名称是否已存在
        if dimension.name in self._dimensions:
            raise DimensionValidationError(
                dimension_name=dimension.name,
                value=dimension,
                reason="维度名称已存在"
            )

        # 注册维度
        self._dimensions[dimension.name] = dimension
        self._dimension_classes[dimension.name] = dimension.__class__

    def register_class(self, dimension_class: Type[IDimension]) -> None:
        """
        注册维度类

        Args:
            dimension_class: 维度类

        Raises:
            DimensionValidationError: 维度类验证失败
        """
        # 创建实例并注册
        try:
            instance = dimension_class()
            self.register(instance)
        except Exception as e:
            raise DimensionValidationError(
                dimension_name=dimension_class.__name__,
                value=dimension_class,
                reason=f"无法创建维度实例: {str(e)}"
            )

    def get_dimension(self, name: str) -> IDimension:
        """
        获取维度

        Args:
            name: 维度名称

        Returns:
            维度实例

        Raises:
            DimensionNotFoundError: 维度不存在
        """
        if name not in self._dimensions:
            raise DimensionNotFoundError(dimension_name=name)

        return self._dimensions[name]

    def create_dimension(self, name: str, **kwargs) -> IDimension:
        """
        创建维度实例

        Args:
            name: 维度名称
            **kwargs: 维度初始化参数

        Returns:
            创建的维度实例

        Raises:
            DimensionNotFoundError: 维度类不存在
        """
        if name not in self._dimension_classes:
            raise DimensionNotFoundError(dimension_name=name)

        dimension_class = self._dimension_classes[name]
        return dimension_class(**kwargs)

    def has_dimension(self, name: str) -> bool:
        """检查维度是否存在"""
        return name in self._dimensions

    def list_dimensions(self) -> List[str]:
        """列出所有可用维度名称"""
        return sorted(list(self._dimensions.keys()))

    def list_dimensions_info(self) -> List[Dict[str, any]]:
        """列出所有维度信息"""
        return [
            {
                "name": dim.name,
                "display_name": dim.display_name,
                "description": dim.description,
                "data_type": dim.data_type.__name__,
                "unit": dim.unit,
                "is_calculated": dim.is_calculated
            }
            for dim in self._dimensions.values()
        ]

    def validate_dimension_data(self, dimension_name: str, value: any) -> bool:
        """
        验证维度数据

        Args:
            dimension_name: 维度名称
            value: 数据值

        Returns:
            是否有效
        """
        if not self.has_dimension(dimension_name):
            return False

        dimension = self.get_dimension(dimension_name)
        return dimension.validate(value)

    def format_dimension_data(self, dimension_name: str, value: any) -> str:
        """
        格式化维度数据

        Args:
            dimension_name: 维度名称
            value: 数据值

        Returns:
            格式化后的字符串
        """
        if not self.has_dimension(dimension_name):
            return str(value)

        dimension = self.get_dimension(dimension_name)
        return dimension.format(value)

    def calculate_dimension(self, dimension_name: str, node: Any,
                            timestamp: Optional[datetime] = None) -> Any:
        """
        计算维度值

        Args:
            dimension_name: 维度名称
            node: 节点对象
            timestamp: 时间戳

        Returns:
            计算后的值

        Raises:
            DimensionNotFoundError: 维度不存在
            NotImplementedError: 维度不是计算维度
        """
        dimension = self.get_dimension(dimension_name)

        if not dimension.is_calculated:
            raise NotImplementedError(
                f"维度 '{dimension_name}' 不是计算维度"
            )

        return dimension.calculate(node, timestamp)

    def clear(self) -> None:
        """清空所有维度（保留内置维度）"""
        # 备份内置维度
        builtin_names = ["standard_gas", "meter_gas", "loss_rate"]
        builtin_dimensions = {
            name: self._dimensions[name]
            for name in builtin_names
            if name in self._dimensions
        }

        # 清空并重新注册内置维度
        self._dimensions.clear()
        self._dimension_classes.clear()

        for dimension in builtin_dimensions.values():
            self.register(dimension)

    def __len__(self) -> int:
        """获取维度数量"""
        return len(self._dimensions)

    def __contains__(self, name: str) -> bool:
        """检查是否包含维度"""
        return name in self._dimensions