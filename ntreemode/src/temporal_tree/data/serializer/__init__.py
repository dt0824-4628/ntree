"""
序列化模块
负责将Python对象转换为可存储格式
"""

from .base import Serializer, Deserializer
from .json_serializer import JSONSerializer
from .binary_serializer import BinarySerializer

__all__ = [
    'Serializer',
    'Deserializer',
    'JSONSerializer',
    'BinarySerializer'
]