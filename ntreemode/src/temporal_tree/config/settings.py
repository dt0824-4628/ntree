"""
系统配置设置
"""
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime

from ..exceptions import ConfigError


@dataclass
class SystemSettings:
    """
    系统配置类
    使用dataclass确保配置的不可变性和类型安全
    """

    # 系统基本配置
    system_name: str = "燃气输差分析系统"
    version: str = "1.0.0"

    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = None
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # 树结构配置
    max_tree_depth: int = 10
    max_children_per_node: int = 100

    # IP编码配置
    ip_base: str = "10.0.0.0"
    ip_max_segments: int = 10
    ip_segment_max_value: int = 255

    # 时间配置
    time_format: str = "%Y-%m-%d %H:%M:%S"
    default_timezone: str = "Asia/Shanghai"

    # 维度配置
    default_dimensions: list = field(default_factory=lambda: [
        "standard_gas",  # 标准气量
        "meter_gas",  # 表计气量
        "loss_rate"  # 输差率
    ])

    # 存储配置
    storage_backend: str = "memory"  # memory, json, sqlite
    storage_path: Optional[str] = None

    # 缓存配置
    enable_cache: bool = True
    cache_size: int = 1000
    cache_ttl: int = 3600  # 1小时

    # 性能配置
    enable_validation: bool = True
    enable_logging: bool = True
    batch_operation_size: int = 100

    def __post_init__(self):
        """初始化后处理，验证配置"""
        self._validate_settings()
        self._set_defaults()

    def _validate_settings(self):
        """验证配置值"""
        # 验证日志级别
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ConfigError(
                message=f"无效的日志级别: {self.log_level}",
                config_key="log_level",
                valid_values=valid_log_levels
            )

        # 验证树深度
        if self.max_tree_depth <= 0 or self.max_tree_depth > 20:
            raise ConfigError(
                message=f"树深度必须在1-20之间: {self.max_tree_depth}",
                config_key="max_tree_depth",
                valid_range="1-20"
            )

        # 验证IP基础地址
        self._validate_ip_format(self.ip_base)

    def _validate_ip_format(self, ip_str: str):
        """验证IP格式"""
        parts = ip_str.split('.')
        if len(parts) > self.ip_max_segments:
            raise ConfigError(
                message=f"IP地址段数超过限制: {len(parts)} > {self.ip_max_segments}",
                config_key="ip_base"
            )

        for part in parts:
            if not part.isdigit():
                raise ConfigError(
                    message=f"IP地址包含非数字字符: {part}",
                    config_key="ip_base"
                )
            if int(part) > self.ip_segment_max_value:
                raise ConfigError(
                    message=f"IP地址段值超过限制: {part} > {self.ip_segment_max_value}",
                    config_key="ip_base"
                )

    def _set_defaults(self):
        """设置默认值"""
        # 设置默认存储路径
        if self.storage_backend in ["json", "sqlite"] and not self.storage_path:
            self.storage_path = os.path.join(
                os.getcwd(),
                "data",
                f"{self.system_name.lower().replace(' ', '_')}.{'json' if self.storage_backend == 'json' else 'db'}"
            )

        # 确保目录存在
        if self.storage_path:
            os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)

        # 设置默认日志文件
        if not self.log_file and self.enable_logging:
            log_dir = os.path.join(os.getcwd(), "logs")
            os.makedirs(log_dir, exist_ok=True)
            self.log_file = os.path.join(
                log_dir,
                f"{datetime.now().strftime('%Y%m%d')}_{self.system_name.lower().replace(' ', '_')}.log"
            )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'SystemSettings':
        """从字典创建配置"""
        # 过滤无效的配置键
        valid_keys = {field.name for field in cls.__dataclass_fields__.values()}
        filtered_config = {k: v for k, v in config_dict.items() if k in valid_keys}

        return cls(**filtered_config)