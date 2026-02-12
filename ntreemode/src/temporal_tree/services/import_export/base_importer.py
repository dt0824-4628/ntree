"""
数据导入器基类
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any
from datetime import datetime
from temporal_tree.exceptions import BaseError


class ImportError(BaseError):
    """导入过程异常"""
    pass


class DataImporter(ABC):
    """数据导入器抽象基类"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self._validate_config()

    def _validate_config(self):
        """验证配置参数"""
        pass

    @abstractmethod
    def validate_file(self, file_path: str) -> bool:
        """验证文件是否可导入"""
        pass

    @abstractmethod
    def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """提取文件元数据"""
        pass

    @abstractmethod
    def parse_data(self, file_path: str) -> List[Dict[str, Any]]:
        """解析数据为标准化格式"""
        pass

    @abstractmethod
    def convert_to_tree_nodes(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """将数据转换为树节点格式"""
        pass

    def import_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        导入数据的完整流程
        1. 验证文件
        2. 解析数据
        3. 转换为树节点
        """
        if not self.validate_file(file_path):
            raise ImportError(f"文件验证失败: {file_path}")

        data = self.parse_data(file_path)
        return self.convert_to_tree_nodes(data)