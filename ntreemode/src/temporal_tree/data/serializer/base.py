"""
序列化基类定义
"""
from abc import ABC, abstractmethod
from typing import Any, Dict


class Serializer(ABC):
    """序列化器抽象基类"""

    @abstractmethod
    def serialize(self, obj: Any) -> bytes:
        """将Python对象序列化为字节流"""
        pass

    @abstractmethod
    def serialize_to_dict(self, obj: Any) -> Dict:
        """将Python对象序列化为字典（用于JSON等）"""
        pass


class Deserializer(ABC):
    """反序列化器抽象基类"""

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """从字节流反序列化为Python对象"""
        pass

    @abstractmethod
    def deserialize_from_dict(self, data_dict: Dict) -> Any:
        """从字典反序列化为Python对象"""
        pass


class SerializationError(Exception):
    """序列化相关异常"""
    pass