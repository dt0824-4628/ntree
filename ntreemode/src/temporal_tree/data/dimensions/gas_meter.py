"""
表计气量维度
实际仪表测量的气量值
"""
from typing import Any, Dict, Optional
from datetime import datetime

from .base import BaseDimension


class MeterGasDimension(BaseDimension):
    """表计气量维度"""

    def __init__(self):
        super().__init__(
            name="meter_gas",
            display_name="表计气量",
            description="实际仪表测量的气量值，反映实际输气情况",
            data_type=float,
            unit="m³",
            is_calculated=False
        )
        self._metadata.update({
            "category": "gas_volume",
            "precision": 2,
            "min_value": 0.0,
            "max_value": 1000000.0,
            "typical_range": "0-1,000,000 m³",
            "measurement_device": "gas_meter",
            "accuracy": "±0.5%"  # 测量精度
        })

    def _validate_impl(self, value: Any) -> bool:
        """验证表计气量值"""
        try:
            num_value = float(value)

            # 表计气量必须为非负数
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
        """格式化表计气量值"""
        if value is None:
            return "表计气量: N/A"

        try:
            num_value = float(value)
            formatted = f"{num_value:,.2f}"
            return f"表计气量: {formatted} {self._unit}"
        except:
            return f"表计气量: {value} {self._unit}"