"""
维度模块
包含燃气输差分析相关的数据维度
"""

from .base import BaseDimension
from .gas_standard import StandardGasDimension
from .gas_meter import MeterGasDimension
from .loss_rate import LossRateDimension
from .registry import DimensionRegistry

__all__ = [
    'BaseDimension',
    'StandardGasDimension',
    'MeterGasDimension',
    'LossRateDimension',
    'DimensionRegistry'
]