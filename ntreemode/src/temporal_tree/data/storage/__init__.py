"""
存储模块
提供多种数据存储后端
"""

from .adapter import DataStoreAdapter, StorageContext
from .memory_store import MemoryStore
from .json_store import JSONStore
from .sqlite_store import SQLiteStore

# 存储类型映射
STORAGE_TYPES = {
    'memory': MemoryStore,
    'json': JSONStore,
    'sqlite': SQLiteStore
}


def create_store(
        store_type: str = 'memory',
        **kwargs
) -> DataStoreAdapter:
    """
    创建存储适配器

    Args:
        store_type: 存储类型 ('memory', 'json', 'sqlite')
        **kwargs: 传递给存储构造函数的参数

    Returns:
        存储适配器实例
    """
    store_class = STORAGE_TYPES.get(store_type.lower())
    if not store_class:
        raise ValueError(f"不支持的存储类型: {store_type}")

    return store_class(**kwargs)


__all__ = [
    'DataStoreAdapter',
    'StorageContext',
    'MemoryStore',
    'JSONStore',
    'SQLiteStore',
    'create_store',
    'STORAGE_TYPES'
]