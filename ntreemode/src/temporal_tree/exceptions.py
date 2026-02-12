"""
燃气输差分析系统异常体系
"""
from datetime import datetime
from typing import Dict, Any, Optional


class BaseError(Exception):
    """所有异常的基类"""
    def __init__(
        self,
        message: str,
        code: str = "UNKNOWN_ERROR",
        details: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.code = code
        self.details = details or {}
        self.context = context or {}
        self.timestamp = datetime.now()
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，便于序列化"""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "context": self.context,
            "timestamp": self.timestamp.isoformat()
        }

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


# ==================== 配置和验证异常 ====================
class ConfigError(BaseError):
    """配置错误"""
    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        details = {"config_key": config_key} if config_key else {}
        super().__init__(message, code="CONFIG_ERROR", details=details, **kwargs)


class ValidationError(BaseError):
    """数据验证错误"""
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        reason: Optional[str] = None,
        **kwargs
    ):
        details = {
            "field": field,
            "value": value,
            "reason": reason
        }
        super().__init__(message, code="VALIDATION_ERROR", details=details, **kwargs)


# ==================== 树结构相关异常 ====================
class TreeError(BaseError):
    """树结构错误基类"""
    pass


class TreeNotFoundError(TreeError):
    """树不存在"""
    def __init__(self, tree_id: str, **kwargs):
        super().__init__(
            message=f"树不存在: {tree_id}",
            code="TREE_NOT_FOUND",
            details={"tree_id": tree_id},
            **kwargs
        )


class NodeError(TreeError):
    """节点操作错误"""
    pass


class NodeNotFoundError(NodeError):
    """节点不存在"""
    def __init__(self, node_id: Optional[str] = None, ip_address: Optional[str] = None, **kwargs):
        details = {}
        if node_id:
            details["node_id"] = node_id
        if ip_address:
            details["ip_address"] = ip_address

        message = f"节点不存在"
        if node_id:
            message += f": id={node_id}"
        elif ip_address:
            message += f": ip={ip_address}"

        super().__init__(message, code="NODE_NOT_FOUND", details=details, **kwargs)


# ==================== IP相关异常 ====================
class IPError(TreeError):
    """IP地址错误"""
    pass


class IPAllocationError(IPError):
    """IP地址分配失败"""
    def __init__(self, parent_ip: Optional[str] = None, reason: Optional[str] = None, **kwargs):
        details = {"parent_ip": parent_ip, "reason": reason}
        super().__init__(
            message=f"IP地址分配失败: {reason or '未知原因'}",
            code="IP_ALLOCATION_ERROR",
            details=details,
            **kwargs
        )


class InvalidIPFormatError(IPError):
    """IP格式无效"""
    def __init__(self, ip_address: str, reason: Optional[str] = None, **kwargs):
        details = {"ip_address": ip_address, "reason": reason}
        message = f"无效的IP格式: {ip_address}"
        if reason:
            message += f" ({reason})"
        super().__init__(message, code="INVALID_IP_FORMAT", details=details, **kwargs)


# ==================== 时间相关异常 ====================
class TimeError(BaseError):
    """时间相关错误基类"""
    pass


class InvalidTimestampError(TimeError):
    """时间戳无效"""
    def __init__(self, timestamp: str, **kwargs):
        super().__init__(
            message=f"无效的时间戳: {timestamp}",
            code="INVALID_TIMESTAMP",
            details={"timestamp": timestamp},
            **kwargs
        )


class TimeTravelError(TimeError):
    """时间旅行错误"""
    def __init__(self, target_time: str = None, reason: str = "", **kwargs):
        details = {"target_time": target_time, "reason": reason}
        super().__init__(
            message=f"时间旅行失败: {reason}" if reason else "时间旅行失败",
            code="TIME_TRAVEL_ERROR",
            details=details,
            **kwargs
        )


class TimelineError(TimeError):
    """时间线操作错误"""
    def __init__(self, timeline_id: str = None, operation: str = None, **kwargs):
        details = {"timeline_id": timeline_id, "operation": operation}
        message = "时间线操作失败"
        if timeline_id and operation:
            message = f"时间线操作失败 [{operation}]: {timeline_id}"
        super().__init__(message, code="TIMELINE_ERROR", details=details, **kwargs)


# ==================== 维度相关异常 ====================
class DimensionError(BaseError):
    """维度数据错误"""
    pass


class DimensionNotFoundError(DimensionError):
    """维度不存在"""
    def __init__(self, dimension_name: str, **kwargs):
        super().__init__(
            message=f"维度不存在: {dimension_name}",
            code="DIMENSION_NOT_FOUND",
            details={"dimension_name": dimension_name},
            **kwargs
        )


class DimensionValidationError(DimensionError):
    """维度数据验证失败"""
    def __init__(self, dimension_name: str, value: Any, reason: str, **kwargs):
        super().__init__(
            message=f"维度'{dimension_name}'数据验证失败: {reason}",
            code="DIMENSION_VALIDATION_ERROR",
            details={
                "dimension_name": dimension_name,
                "value": value,
                "reason": reason
            },
            **kwargs
        )


# ==================== 存储相关异常 ====================
class StorageError(BaseError):
    """存储错误"""
    pass


class StorageNotFoundError(StorageError):
    """存储未找到"""
    def __init__(self, storage_type: str, **kwargs):
        super().__init__(
            message=f"存储类型不存在: {storage_type}",
            code="STORAGE_NOT_FOUND",
            details={"storage_type": storage_type},
            **kwargs
        )


class DataStoreError(StorageError):
    """数据存储异常"""
    def __init__(self, message: str, operation: str = None, store_type: str = None, **kwargs):
        details = {"operation": operation, "store_type": store_type}
        super().__init__(
            message=f"存储错误[{operation or 'unknown'}]: {message}",
            code="DATA_STORE_ERROR",
            details=details,
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


# ==================== 查询相关异常 ====================
class QueryError(BaseError):
    """查询错误"""
    pass


class InvalidQueryError(QueryError):
    """查询语句无效"""
    def __init__(self, query: str, reason: str, **kwargs):
        super().__init__(
            message=f"查询无效: {reason}",
            code="INVALID_QUERY",
            details={"query": query, "reason": reason},
            **kwargs
        )


# ==================== 系统运行时异常 ====================
class SystemError(BaseError):
    """系统运行时错误"""
    pass


class InitializationError(SystemError):
    """系统初始化错误"""
    def __init__(self, component: str, reason: str, **kwargs):
        super().__init__(
            message=f"系统组件初始化失败: {component} - {reason}",
            code="INITIALIZATION_ERROR",
            details={"component": component, "reason": reason},
            **kwargs
        )