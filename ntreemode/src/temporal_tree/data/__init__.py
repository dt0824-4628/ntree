"""
数据模块
包含维度、存储、序列化等数据相关功能
"""

from .dimensions import (
    BaseDimension,
    StandardGasDimension,
    MeterGasDimension,
    LossRateDimension,
    DimensionRegistry
)

__all__ = [
    'BaseDimension',
    'StandardGasDimension',
    'MeterGasDimension',
    'LossRateDimension',
    'DimensionRegistry'
]