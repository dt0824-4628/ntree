"""
配置验证器
"""
import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..exceptions import ValidationError, ConfigError


class ConfigValidator:
    """配置验证器"""

    def __init__(self):
        self._ip_pattern = re.compile(r'^(\d+\.)*\d+$')

    def validate_system_config(self, config: Dict[str, Any]) -> bool:
        """验证系统配置"""
        try:
            # 验证必需字段
            required_fields = ['system_name', 'storage_backend']
            for field in required_fields:
                if field not in config:
                    raise ValidationError(
                        message=f"缺少必需配置项: {field}",
                        field=field,
                        reason="required_field_missing"
                    )

            # 验证存储后端
            valid_storages = ['memory', 'json', 'sqlite']
            if config.get('storage_backend') not in valid_storages:
                raise ValidationError(
                    message=f"无效的存储后端: {config.get('storage_backend')}",
                    field="storage_backend",
                    value=config.get('storage_backend'),
                    reason=f"必须是 {valid_storages} 之一"
                )

            # 验证IP基础地址
            if 'ip_base' in config:
                self.validate_ip_address(config['ip_base'])

            # 验证时间格式
            if 'time_format' in config:
                self.validate_time_format(config['time_format'])

            return True

        except ValidationError:
            raise
        except Exception as e:
            raise ConfigError(f"配置验证失败: {str(e)}")

    def validate_tree_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证并清理树配置"""
        validated_config = {}

        # 树名称验证
        if 'name' not in config:
            raise ValidationError(
                message="树配置缺少名称",
                field="name",
                reason="required_field_missing"
            )

        tree_name = config['name']
        if not isinstance(tree_name, str) or len(tree_name) < 1 or len(tree_name) > 100:
            raise ValidationError(
                message="树名称必须是1-100个字符的字符串",
                field="name",
                value=tree_name,
                reason="invalid_length"
            )

        validated_config['name'] = tree_name

        # 树描述（可选）
        if 'description' in config:
            desc = config['description']
            if isinstance(desc, str) and len(desc) <= 500:
                validated_config['description'] = desc

        # 根节点配置
        if 'root_config' in config:
            root_config = config['root_config']
            if isinstance(root_config, dict):
                validated_config['root_config'] = self.validate_node_config(root_config)

        # 维度配置
        if 'dimensions' in config:
            dimensions = config['dimensions']
            if isinstance(dimensions, list):
                validated_config['dimensions'] = self.validate_dimensions_list(dimensions)

        # IP编码配置
        if 'ip_config' in config:
            ip_config = config['ip_config']
            if isinstance(ip_config, dict):
                validated_config['ip_config'] = self.validate_ip_config(ip_config)

        return validated_config

    def validate_node_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证节点配置"""
        validated = {}

        # 节点名称
        if 'name' not in config:
            raise ValidationError(
                message="节点配置缺少名称",
                field="name",
                reason="required_field_missing"
            )

        name = config['name']
        if not self._validate_string(name, min_len=1, max_len=100):
            raise ValidationError(
                message="节点名称必须是1-100个字符的字符串",
                field="name",
                value=name,
                reason="invalid_name"
            )

        validated['name'] = name

        # 节点标签（可选）
        if 'tags' in config:
            tags = config['tags']
            if isinstance(tags, list):
                validated['tags'] = [str(tag) for tag in tags if self._validate_string(str(tag))]

        # 节点数据（可选）
        if 'initial_data' in config:
            data = config['initial_data']
            if isinstance(data, dict):
                validated['initial_data'] = data

        return validated

    def validate_dimensions_list(self, dimensions: List[str]) -> List[str]:
        """验证维度列表"""
        validated_dimensions = []

        for dim in dimensions:
            if not isinstance(dim, str):
                raise ValidationError(
                    message="维度名称必须是字符串",
                    field="dimensions",
                    value=dim,
                    reason="invalid_type"
                )

            # 验证维度名称格式
            if not self._validate_dimension_name(dim):
                raise ValidationError(
                    message=f"无效的维度名称格式: {dim}",
                    field="dimensions",
                    value=dim,
                    reason="invalid_format"
                )

            validated_dimensions.append(dim)

        return validated_dimensions

    def validate_ip_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证IP配置"""
        validated = {}

        # IP基础地址
        if 'base_address' in config:
            base_ip = config['base_address']
            self.validate_ip_address(base_ip)
            validated['base_address'] = base_ip

        # 最大层级
        if 'max_depth' in config:
            max_depth = config['max_depth']
            if isinstance(max_depth, int) and 1 <= max_depth <= 20:
                validated['max_depth'] = max_depth

        # 每层最大子节点数
        if 'max_children_per_level' in config:
            max_children = config['max_children_per_level']
            if isinstance(max_children, int) and max_children > 0:
                validated['max_children_per_level'] = min(max_children, 1000)

        return validated

    def validate_ip_address(self, ip_address: str) -> bool:
        """验证IP地址格式"""
        if not isinstance(ip_address, str):
            raise ValidationError(
                message="IP地址必须是字符串",
                field="ip_address",
                value=ip_address,
                reason="invalid_type"
            )

        if not self._ip_pattern.match(ip_address):
            raise ValidationError(
                message="IP地址格式无效",
                field="ip_address",
                value=ip_address,
                reason="invalid_format"
            )

        parts = ip_address.split('.')
        if len(parts) > 10:
            raise ValidationError(
                message="IP地址层级过多",
                field="ip_address",
                value=ip_address,
                reason="too_many_segments"
            )

        for part in parts:
            if not part.isdigit():
                raise ValidationError(
                    message="IP地址段必须为数字",
                    field="ip_address",
                    value=part,
                    reason="non_numeric_segment"
                )

            value = int(part)
            if value > 255:
                raise ValidationError(
                    message="IP地址段值不能超过255",
                    field="ip_address",
                    value=part,
                    reason="segment_too_large"
                )

        return True

    def validate_time_format(self, time_format: str) -> bool:
        """验证时间格式"""
        if not isinstance(time_format, str):
            return False

        try:
            # 尝试使用该格式格式化当前时间
            datetime.now().strftime(time_format)
            return True
        except:
            raise ValidationError(
                message="无效的时间格式",
                field="time_format",
                value=time_format,
                reason="invalid_format_string"
            )

    def _validate_string(self, value: str, min_len: int = 1, max_len: int = 100) -> bool:
        """验证字符串"""
        return isinstance(value, str) and min_len <= len(value) <= max_len

    def _validate_dimension_name(self, name: str) -> bool:
        """验证维度名称格式"""
        if not isinstance(name, str):
            return False

        # 只允许字母、数字、下划线，且以字母开头
        pattern = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
        return bool(pattern.match(name)) and 1 <= len(name) <= 50