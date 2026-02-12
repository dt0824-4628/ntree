"""
标准气量维度
设计标准计算的理论气量值
"""
from typing import Any, Dict, Optional
from datetime import datetime

from .base import BaseDimension


class StandardGasDimension(BaseDimension):
    """标准气量维度"""

    def __init__(self):
        super().__init__(
            name="standard_gas",
            display_name="标准气量",
            description="根据设计标准计算的理论气量值，用于输差分析",
            data_type=float,
            unit="m³",
            is_calculated=False
        )
        self._metadata.update({
            "category": "gas_volume",
            "precision": 2,  # 小数点后2位
            "min_value": 0.0,
            "max_value": 1000000.0,  # 1百万立方米
            "typical_range": "0-1,000,000 m³"
        })

    def _validate_impl(self, value: Any) -> bool:
        """验证标准气量值"""
        try:
            num_value = float(value)

            # 标准气量必须为非负数
            if num_value < 0:
                return False

            # 检查最大值限制
            if num_value > self._metadata["max_value"]:
                return False

            return True

        except (ValueError, TypeError):
            return False

    def get_valid_range(self) -> Optional[Dict[str, Any]]:
        """获取有效范围"""
        return {
            "min": self._metadata["min_value"],
            "max": self._metadata["max_value"],
            "unit": self._unit
        }

    def format(self, value: Any) -> str:
        """格式化标准气量值"""
        if value is None:
            return "标准气量: N/A"

        try:
            num_value = float(value)
            # 千位分隔符格式化
            formatted = f"{num_value:,.2f}"
            return f"标准气量: {formatted} {self._unit}"
        except:
            return f"标准气量: {value} {self._unit}"