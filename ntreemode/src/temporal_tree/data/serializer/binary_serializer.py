"""
二进制序列化器
使用pickle进行高效序列化，但不可读
"""
import pickle
import zlib
from typing import Any, Dict

from .base import Serializer, Deserializer, SerializationError


class BinarySerializer(Serializer, Deserializer):
    """二进制序列化器（使用pickle）"""

    def __init__(self,
                 protocol: int = pickle.HIGHEST_PROTOCOL,
                 compress: bool = False):
        """
        初始化二进制序列化器

        Args:
            protocol: pickle协议版本
            compress: 是否压缩数据
        """
        self.protocol = protocol
        self.compress = compress

    def serialize(self, obj: Any) -> bytes:
        """序列化为字节流"""
        try:
            data = pickle.dumps(obj, protocol=self.protocol)
            if self.compress:
                data = zlib.compress(data)
            return data
        except Exception as e:
            raise SerializationError(f"二进制序列化失败: {e}")

    def serialize_to_dict(self, obj: Any) -> Dict:
        """二进制序列化不直接支持字典格式"""
        raise NotImplementedError(
            "BinarySerializer不支持序列化为字典，请使用serialize()方法"
        )

    def deserialize(self, data: bytes) -> Any:
        """从字节流反序列化"""
        try:
            if self.compress:
                data = zlib.decompress(data)
            return pickle.loads(data)
        except Exception as e:
            raise SerializationError(f"二进制反序列化失败: {e}")

    def deserialize_from_dict(self, data_dict: Dict) -> Any:
        """二进制序列化不直接支持字典格式"""
        raise NotImplementedError(
            "BinarySerializer不支持从字典反序列化，请使用deserialize()方法"
        )

    def save_to_file(self, obj: Any, filepath: str) -> None:
        """保存对象到文件"""
        data = self.serialize(obj)
        with open(filepath, 'wb') as f:
            f.write(data)

    def load_from_file(self, filepath: str) -> Any:
        """从文件加载对象"""
        with open(filepath, 'rb') as f:
            data = f.read()
        return self.deserialize(data)


# 创建默认实例
default_binary_serializer = BinarySerializer(compress=True)