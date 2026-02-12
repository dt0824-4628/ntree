"""
存储模块专用异常
"""
# 删除这行
# from temporal_tree.exceptions import TemporalTreeError

# 改为导入 BaseError
from temporal_tree.exceptions import BaseError


class DataStoreError(BaseError):
    """数据存储异常"""
    def __init__(self, message: str, operation: str = None, store_type: str = None, **kwargs):
        details = {"operation": operation, "store_type": store_type}
        super().__init__(
            message=f"存储错误[{operation or 'unknown'}]: {message}",
            code="DATA_STORE_ERROR",
            details=details,
            **kwargs
        )


class StorageConnectionError(DataStoreError):
    """存储连接异常"""
    def __init__(self, message: str, store_type: str, **kwargs):
        super().__init__(
            message=f"存储连接失败: {message}",
            operation="CONNECT",
            store_type=store_type,
            **kwargs
        )


class StorageOperationError(DataStoreError):
    """存储操作异常"""
    def __init__(self, message: str, operation: str, store_type: str, **kwargs):
        super().__init__(
            message=f"存储操作失败[{operation}]: {message}",
            operation=operation,
            store_type=store_type,
            **kwargs
        )


class SerializationError(DataStoreError):
    """序列化异常"""
    def __init__(self, message: str, data_type: str = None, **kwargs):
        details = {"data_type": data_type}
        super().__init__(
            message=f"序列化错误: {message}",
            operation="SERIALIZATION",
            store_type="serialization",
            details=details,
            **kwargs
        )