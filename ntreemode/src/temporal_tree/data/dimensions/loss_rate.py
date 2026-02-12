"""
输差率维度
计算公式: (标准气量 - 表计气量) / 标准气量 × 100%
"""

from typing import Any, Dict, Optional
from .base import BaseDimension  # ✅ 添加这行导入！


class LossRateDimension(BaseDimension):
    """输差率维度"""

    def __init__(self):
        super().__init__(
            name="loss_rate",
            display_name="输差率",
            description="输差率 = (标准气量 - 表计气量) / 标准气量 × 100%",
            data_type=float,
            unit="%",
            is_calculated=True
        )
        self._metadata.update({
            "category": "gas_loss",
            "precision": 2,
            "min_value": -100.0,
            "max_value": 100.0,
            "warning_threshold": 5.0,
            "alarm_threshold": 10.0
        })

    def _validate_impl(self, value: Any) -> bool:
        """验证输差率值"""
        try:
            num_value = float(value)
            if num_value < -100.0 or num_value > 100.0:
                return False
            return True
        except (ValueError, TypeError):
            return False

    def calculate(self, standard_gas: float, meter_gas: float) -> float:
        """计算输差率"""
        if standard_gas == 0:
            return 0.0
        return ((standard_gas - meter_gas) / standard_gas) * 100

    def get_warning_level(self, value: float) -> str:
        """获取告警级别（使用绝对值）"""
        abs_value = abs(value)  # ✅ 负值也用绝对值判断告警
        if abs_value >= self._metadata["alarm_threshold"]:
            return "ALARM"
        elif abs_value >= self._metadata["warning_threshold"]:
            return "WARNING"
        return "NORMAL"

    def format(self, value: Any) -> str:
        """格式化输差率"""
        if value is None:
            return "输差率: N/A"

        try:
            num_value = float(value)
            level = self.get_warning_level(num_value)
            formatted = f"{num_value:.2f}"

            if level == "ALARM":
                return f"🔴 输差率: {formatted}% (报警)"
            elif level == "WARNING":
                return f"🟡 输差率: {formatted}% (警告)"
            else:
                return f"✅ 输差率: {formatted}% (正常)"
        except:
            return f"输差率: {value}%"