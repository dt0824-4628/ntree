"""
JSON序列化器
使用标准json模块进行序列化
"""
import json
import pickle
from datetime import datetime, date
from typing import Any, Dict
from decimal import Decimal
from pathlib import Path

from .base import Serializer, Deserializer, SerializationError


class DateTimeEncoder(json.JSONEncoder):
    """处理日期时间对象的JSON编码器"""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return {
                '__type__': 'datetime' if isinstance(obj, datetime) else 'date',
                'value': obj.isoformat()
            }
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, Path):
            return str(obj)
        elif hasattr(obj, 'to_dict'):
            # 支持自定义序列化对象
            return obj.to_dict()

        return super().default(obj)


class JSONSerializer(Serializer, Deserializer):
    """JSON序列化器"""

    def __init__(self,
                 ensure_ascii: bool = False,
                 indent: int = 2,
                 sort_keys: bool = True,
                 datetime_format: str = "iso"):
        """
        初始化JSON序列化器

        Args:
            ensure_ascii: 是否确保ASCII编码
            indent: 缩进空格数
            sort_keys: 是否按键排序
            datetime_format: 日期时间格式
        """
        self.ensure_ascii = ensure_ascii
        self.indent = indent
        self.sort_keys = sort_keys
        self.datetime_format = datetime_format

    def serialize(self, obj: Any) -> bytes:
        """序列化为字节流"""
        try:
            dict_data = self.serialize_to_dict(obj)
            json_str = json.dumps(
                dict_data,
                ensure_ascii=self.ensure_ascii,
                indent=self.indent,
                sort_keys=self.sort_keys,
                cls=DateTimeEncoder
            )
            return json_str.encode('utf-8')
        except Exception as e:
            raise SerializationError(f"JSON序列化失败: {e}")

    def serialize_to_dict(self, obj: Any) -> Dict:
        """序列化为字典"""
        # 如果对象有to_dict方法，使用它
        if hasattr(obj, 'to_dict') and callable(obj.to_dict):
            return obj.to_dict()

        # 处理特殊类型
        if isinstance(obj, datetime):
            if self.datetime_format == "iso":
                return obj.isoformat()  # 直接返回ISO格式字符串
            else:
                return {
                    '__type__': 'datetime',
                    'value': obj.strftime(self.datetime_format)
                }
        elif isinstance(obj, date):
            return obj.isoformat()  # 直接返回ISO格式

        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, Path):
            return str(obj)
        elif isinstance(obj, (list, tuple, dict, str, int, float, bool, type(None))):
            # 基本类型直接返回
            return obj
        else:
            # 尝试转换为字典
            try:
                return dict(obj)
            except:
                # 最后使用pickle作为备选
                return {
                    '__type__': 'pickle',
                    'value': pickle.dumps(obj).hex()
                }

    def deserialize(self, data: bytes) -> Any:
        """从字节流反序列化"""
        try:
            json_str = data.decode('utf-8')
            dict_data = json.loads(json_str)
            return self.deserialize_from_dict(dict_data)
        except Exception as e:
            raise SerializationError(f"JSON反序列化失败: {e}")

    def deserialize_from_dict(self, data_dict: Dict) -> Any:
        """从字典反序列化"""
        if not isinstance(data_dict, dict):
            # 如果是字符串，可能是ISO格式的日期时间
            if isinstance(data_dict, str):
                try:
                    # 尝试解析ISO格式的日期时间
                    return datetime.fromisoformat(data_dict)
                except:
                    pass
            return data_dict

        # 处理特殊类型标记
        if '__type__' in data_dict:
            type_name = data_dict['__type__']
            value = data_dict['value']

            if type_name == 'datetime':
                if self.datetime_format == "iso":
                    return datetime.fromisoformat(value)
                else:
                    return datetime.strptime(value, self.datetime_format)
            elif type_name == 'date':
                return datetime.fromisoformat(value).date()
            elif type_name == 'pickle':
                # 使用pickle反序列化
                return pickle.loads(bytes.fromhex(value))

        # 递归处理嵌套结构
        if isinstance(data_dict, dict):
            for key, value in data_dict.items():
                if isinstance(value, dict):
                    data_dict[key] = self.deserialize_from_dict(value)
                elif isinstance(value, list):
                    data_dict[key] = [self.deserialize_from_dict(item)
                                      if isinstance(item, dict) else item
                                      for item in value]
                elif isinstance(value, str):
                    # 尝试解析ISO格式的日期时间字符串
                    try:
                        data_dict[key] = datetime.fromisoformat(value)
                    except:
                        pass

        return data_dict

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
default_json_serializer = JSONSerializer()