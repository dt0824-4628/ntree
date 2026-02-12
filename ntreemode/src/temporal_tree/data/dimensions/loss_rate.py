"""
输差率维度
计算维度：(标准气量 - 表计气量) / 标准气量
"""
from typing import Any, Dict, Optional
from datetime import datetime

from .base import BaseDimension


class LossRateDimension(BaseDimension):
    """输差率维度（计算维度）"""

    def __init__(self):
        super().__init__(
            name="loss_rate",
            display_name="输差率",
            description="燃气输差率，计算公式：(标准气量 - 表计气量) / 标准气量",
            data_type=float,
            unit="%",
            is_calculated=True
        )
        self._metadata.update({
            "category": "calculated",
            "precision": 4,  # 小数点后4位（百分比需要更高精度）
            "min_value": -1.0,  # 允许负值（表示表计气量大于标准气量）
            "max_value": 1.0,
            "formula": "(standard_gas - meter_gas) / standard_gas",
            "warning_threshold": 0.05,  # 5%警告阈值
            "alarm_threshold": 0.10,  # 10%报警阈值
            "optimal_range": "0-3%"  # 理想输差范围
        })

    def _validate_impl(self, value: Any) -> bool:
        """验证输差率值"""
        try:
            num_value = float(value)

            # 输差率通常在 -100% 到 100% 之间
            if num_value < -1.0 or num_value > 1.0:
                return False

            return True

        except (ValueError, TypeError):
            return False

    def _calculate_impl(self, node: Any, timestamp: Optional[datetime] = None) -> Any:
        """
        计算输差率

        Args:
            node: 节点对象，需要包含standard_gas和meter_gas维度
            timestamp: 时间戳

        Returns:
            输差率（小数形式，如0.05表示5%）
        """
        # 获取标准气量
        standard_gas = node.get_data("standard_gas", timestamp)
        if standard_gas is None:
            return None

        # 获取表计气量
        meter_gas = node.get_data("meter_gas", timestamp)
        if meter_gas is None:
            return None

        # 避免除零错误
        if standard_gas == 0:
            return 0.0

        # 计算输差率
        loss_rate = (standard_gas - meter_gas) / standard_gas
        return loss_rate

    def get_valid_range(self) -> Optional[Dict[str, Any]]:
        """获取有效范围"""
        return {
            "min": -1.0,
            "max": 1.0,
            "unit": self._unit,
            "warning_threshold": self._metadata["warning_threshold"],
            "alarm_threshold": self._metadata["alarm_threshold"]
        }

    def format(self, value: Any) -> str:
        """格式化输差率值"""
        if value is None:
            return "输差率: N/A"

        try:
            num_value = float(value)
            percentage = num_value * 100  # 转换为百分比

            # 判断输差等级
            if percentage > self._metadata["alarm_threshold"] * 100:
                level = "⚠ 报警"
            elif percentage > self._metadata["warning_threshold"] * 100:
                level = "⚠ 警告"
            else:
                level = "✓ 正常"

            return f"输差率: {percentage:.2f}% {level}"
        except:
            return f"输差率: {value} {self._unit}"

    def get_loss_level(self, value: float) -> str:
        """
        获取输差等级

        Args:
            value: 输差率值（小数形式）

        Returns:
            等级描述：'normal', 'warning', 'alarm'
        """
        if value > self._metadata["alarm_threshold"]:
            return "alarm"
        elif value > self._metadata["warning_threshold"]:
            return "warning"
        else:
            return "normal"