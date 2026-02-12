"""
接口定义包
"""

from .inode import INode
from .iprovider import IIPProvider
from .idatastore import IDataStore
from .idimension import IDimension
from .iquery import IQuery, IQueryBuilder

__all__ = [
    'INode',
    'IIPProvider',
    'IDataStore',
    'IDimension',
    'IQuery',
    'IQueryBuilder'
]