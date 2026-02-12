# -*- coding: utf-8 -*-
"""
燃气输差分析系统主入口
"""
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

from .exceptions import (
    TreeError, TreeNotFoundError, NodeError, 
    ValidationError, ConfigError
)
from .config.settings import SystemSettings
from .config.validator import ConfigValidator

from .core.ip import IncrementalIPProvider
from .core.node import NodeFactory, NodeRepository, TreeNode
from .data.dimensions import DimensionRegistry


class TemporalTreeSystem:
    """
    燃气输差分析系统主类
    集成IP模块、节点模块、维度模块，提供完整的管理接口
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化系统
        
        Args:
            config: 系统配置字典
        """
        # 加载配置
        self.settings = SystemSettings.from_dict(config) if config else SystemSettings()
        self.validator = ConfigValidator()
        
        # 初始化日志
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # 核心组件
        self._ip_provider: Optional[IncrementalIPProvider] = None
        self._node_factory: Optional[NodeFactory] = None
        self._dimension_registry: Optional[DimensionRegistry] = None
        self._trees: Dict[str, NodeRepository] = {}  # tree_id -> NodeRepository
        self._tree_metadata: Dict[str, Dict[str, Any]] = {}
        
        # 系统状态
        self._initialized = False
        self._start_time = datetime.now()
        
        self.logger.info("燃气输差分析系统初始化完成")
    
    def _setup_logging(self):
        """配置日志系统"""
        logging.basicConfig(
            level=getattr(logging, self.settings.log_level),
            format=self.settings.log_format,
            handlers=[
                logging.StreamHandler(),
            ]
        )
    
    def initialize(self):
        """初始化系统组件"""
        if self._initialized:
            return
        
        try:
            # 初始化IP提供者
            self._ip_provider = IncrementalIPProvider(
                base_ip=self.settings.ip_base,
                max_depth=self.settings.max_tree_depth,
                max_children_per_node=self.settings.max_children_per_node
            )
            
            # 初始化节点工厂
            self._node_factory = NodeFactory(self._ip_provider)
            
            # 初始化维度注册表
            self._dimension_registry = DimensionRegistry()
            
            # 注册默认维度计算器
            self._setup_default_calculators()
            
            self._initialized = True
            self.logger.info("系统组件初始化完成")
            
        except Exception as e:
            self.logger.error(f"系统初始化失败: {e}")
            raise
    
    def _setup_default_calculators(self):
        """设置默认维度计算器"""
        if not self._dimension_registry:
            return
        
        # 为所有节点添加输差率计算器
        def make_loss_rate_calculator():
            registry = self._dimension_registry
            def calculator(node, timestamp=None):
                return registry.calculate_dimension("loss_rate", node, timestamp)
            return calculator
        
        self._default_calculators = {
            "loss_rate": make_loss_rate_calculator()
        }
    
    def create_tree(self, tree_id: str, name: str, 
                    description: str = "", metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        创建新的分析树
        
        Args:
            tree_id: 树ID
            name: 树名称
            description: 树描述
            metadata: 额外元数据
            
        Returns:
            创建结果
        """
        if not self._initialized:
            raise RuntimeError("系统未初始化，请先调用initialize()方法")
        
        if tree_id in self._trees:
            raise TreeError(f"树已存在: {tree_id}")
        
        try:
            # 创建根节点
            root_metadata = metadata or {}
            root_metadata.update({
                "tree_id": tree_id,
                "description": description
            })
            
            root_node = self._node_factory.create_root_node(name, root_metadata)
            
            # 创建节点仓库
            repository = NodeRepository(root_node)
            
            # 为根节点添加默认计算器
            for dim_name, calculator in self._default_calculators.items():
                root_node.add_dimension_calculator(dim_name, calculator)
            
            # 保存树
            self._trees[tree_id] = repository
            self._tree_metadata[tree_id] = {
                "id": tree_id,
                "name": name,
                "description": description,
                "created_at": datetime.now().isoformat(),
                "root_node_id": root_node.node_id,
                "node_count": repository.get_node_count(),
                "metadata": root_metadata
            }
            
            self.logger.info(f"创建树成功: {tree_id} ({name})")
            
            return {
                "success": True,
                "tree_id": tree_id,
                "name": name,
                "root_node": root_node.to_dict(),
                "created_at": self._tree_metadata[tree_id]["created_at"]
            }
            
        except Exception as e:
            self.logger.error(f"创建树失败: {tree_id}, 错误: {e}")
            raise
    
    def get_tree(self, tree_id: str) -> NodeRepository:
        """获取树"""
        if tree_id not in self._trees:
            raise TreeNotFoundError(tree_id=tree_id)
        return self._trees[tree_id]
    
    def add_node(self, tree_id: str, parent_node_id: str, 
                 name: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        添加节点到树
        
        Args:
            tree_id: 树ID
            parent_node_id: 父节点ID
            name: 节点名称
            metadata: 节点元数据
            
        Returns:
            添加结果
        """
        repository = self.get_tree(tree_id)
        parent = repository.get_node(parent_node_id)
        
        if not parent:
            raise NodeError(f"父节点不存在: {parent_node_id}")
        
        try:
            # 创建子节点
            node = self._node_factory.create_child_node(parent, name, metadata)
            
            # 添加默认计算器
            for dim_name, calculator in self._default_calculators.items():
                node.add_dimension_calculator(dim_name, calculator)
                repository.add_node(node)
                
            # 更新树元数据
            self._tree_metadata[tree_id]["node_count"] = repository.get_node_count()
            
            self.logger.info(f"添加节点成功: {name} 到树 {tree_id}")
            
            return {
                "success": True,
                "node_id": node.node_id,
                "name": node.name,
                "ip_address": node.ip_address,
                "parent_id": parent_node_id,
                "tree_id": tree_id
            }
            
        except Exception as e:
            self.logger.error(f"添加节点失败: {name}, 错误: {e}")
            raise
    
    def set_node_data(self, tree_id: str, node_id: str, 
                      dimension: str, value: Any, 
                      timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """
        设置节点数据
        
        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            value: 数据值
            timestamp: 时间戳
            
        Returns:
            设置结果
        """
        repository = self.get_tree(tree_id)
        node = repository.get_node(node_id)
        
        if not node:
            raise NodeError(f"节点不存在: {node_id}")
        
        # 验证数据
        if not self._dimension_registry.validate_dimension_data(dimension, value):
            raise ValidationError(
                message=f"维度数据验证失败: {dimension}",
                field=dimension,
                value=value,
                reason="invalid_data"
            )
        
        try:
            # 设置数据
            node.set_data(dimension, value, timestamp)
            
            self.logger.info(f"设置节点数据成功: {node_id}.{dimension} = {value}")
            
            return {
                "success": True,
                "node_id": node_id,
                "dimension": dimension,
                "value": value,
                "timestamp": timestamp.isoformat() if timestamp else datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"设置节点数据失败: {node_id}.{dimension}, 错误: {e}")
            raise
    
    def calculate_node_dimension(self, tree_id: str, node_id: str, 
                                 dimension: str, 
                                 timestamp: Optional[datetime] = None) -> Any:
        """
        计算节点维度值
        
        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            timestamp: 时间戳
            
        Returns:
            计算值
        """
        repository = self.get_tree(tree_id)
        node = repository.get_node(node_id)
        
        if not node:
            raise NodeError(f"节点不存在: {node_id}")
        
        if not self._dimension_registry.has_dimension(dimension):
            raise ValidationError(
                message=f"维度不存在: {dimension}",
                field="dimension",
                value=dimension,
                reason="dimension_not_found"
            )
        
        # 如果是计算维度，使用注册表计算
        dimension_obj = self._dimension_registry.get_dimension(dimension)
        if dimension_obj.is_calculated:
            return self._dimension_registry.calculate_dimension(dimension, node, timestamp)
        else:
            # 非计算维度，直接获取数据
            return node.get_data(dimension, timestamp)
    
    def analyze_loss_rate(self, tree_id: str, 
                          threshold: float = 0.05) -> Dict[str, Any]:
        """
        分析树中所有节点的输差率
        
        Args:
            tree_id: 树ID
            threshold: 输差率阈值（默认5%）
            
        Returns:
            分析结果
        """
        repository = self.get_tree(tree_id)
        
        total_standard = 0
        total_meter = 0
        node_count = 0
        high_loss_nodes = []
        
        for node in repository.get_all_nodes():
            standard = node.get_data("standard_gas") or 0
            meter = node.get_data("meter_gas") or 0
            loss_rate = node.get_data("loss_rate") or 0
            
            if standard > 0:  # 只统计有标准气量的节点
                total_standard += standard
                total_meter += meter
                node_count += 1
            
            if loss_rate > threshold:
                high_loss_nodes.append({
                    "node_id": node.node_id,
                    "name": node.name,
                    "ip_address": node.ip_address,
                    "loss_rate": loss_rate,
                    "loss_rate_percent": f"{loss_rate * 100:.2f}%",
                    "standard_gas": standard,
                    "meter_gas": meter
                })
        
        # 计算总体输差率
        overall_loss_rate = 0
        if total_standard > 0:
            overall_loss_rate = (total_standard - total_meter) / total_standard
        
        return {
            "tree_id": tree_id,
            "analysis_time": datetime.now().isoformat(),
            "overall": {
                "total_standard_gas": total_standard,
                "total_meter_gas": total_meter,
                "loss_rate": overall_loss_rate,
                "loss_rate_percent": f"{overall_loss_rate * 100:.2f}%",
                "node_count": node_count
            },
            "high_loss_nodes": high_loss_nodes,
            "threshold": threshold,
            "high_loss_count": len(high_loss_nodes)
        }
    
    def export_tree(self, tree_id: str, 
                    include_data: bool = True,
                    format: str = "dict") -> Any:
        """
        导出树数据
        
        Args:
            tree_id: 树ID
            include_data: 是否包含维度数据
            format: 导出格式，支持 "dict", "json"
            
        Returns:
            导出的数据
        """
        repository = self.get_tree(tree_id)
        
        if format == "dict":
            return repository.to_dict(include_data=include_data)
        elif format == "json":
            tree_dict = repository.to_dict(include_data=include_data)
            return json.dumps(tree_dict, ensure_ascii=False, indent=2)
        else:
            raise ValueError(f"不支持的导出格式: {format}")
    
    def import_tree(self, tree_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        导入树数据
        
        Args:
            tree_id: 树ID
            data: 树数据
            
        Returns:
            导入结果
        """
        if tree_id in self._trees:
            raise TreeError(f"树已存在: {tree_id}")
        
        try:
            # TODO: 实现树数据导入
            # 这需要从字典数据重建整个树结构
            self.logger.warning("树数据导入功能尚未实现")
            
            return {
                "success": False,
                "message": "树数据导入功能尚未实现",
                "tree_id": tree_id
            }
            
        except Exception as e:
            self.logger.error(f"导入树失败: {tree_id}, 错误: {e}")
            raise
    
    def list_trees(self) -> List[Dict[str, Any]]:
        """列出所有树"""
        trees = []
        for tree_id, metadata in self._tree_metadata.items():
            repo = self._trees[tree_id]
            trees.append({
                "tree_id": tree_id,
                "name": metadata["name"],
                "description": metadata.get("description", ""),
                "created_at": metadata["created_at"],
                "node_count": repo.get_node_count(),
                "tree_depth": repo.get_tree_depth(),
                "root_node": metadata.get("root_node_id")
            })
        return trees
    
    def delete_tree(self, tree_id: str) -> Dict[str, Any]:
        """删除树"""
        if tree_id not in self._trees:
            raise TreeNotFoundError(tree_id=tree_id)
        
        # 删除树
        del self._trees[tree_id]
        del self._tree_metadata[tree_id]
        
        self.logger.info(f"删除树成功: {tree_id}")
        
        return {
            "success": True,
            "tree_id": tree_id,
            "deleted_at": datetime.now().isoformat()
        }
    
    def get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        total_nodes = sum(repo.get_node_count() for repo in self._trees.values())
        
        return {
            "system_name": "燃气输差分析系统",
            "version": "1.0.0",
            "start_time": self._start_time.isoformat(),
            "uptime": str(datetime.now() - self._start_time),
            "initialized": self._initialized,
            "tree_count": len(self._trees),
            "total_nodes": total_nodes,
            "dimensions": self._dimension_registry.list_dimensions_info() if self._dimension_registry else [],
            "settings": {
                "ip_base": self.settings.ip_base,
                "max_tree_depth": self.settings.max_tree_depth,
                "max_children_per_node": self.settings.max_children_per_node,
                "default_dimensions": self.settings.default_dimensions
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """系统健康检查"""
        status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {
                "ip_provider": self._ip_provider is not None,
                "node_factory": self._node_factory is not None,
                "dimension_registry": self._dimension_registry is not None,
                "initialized": self._initialized
            },
            "trees": {
                "count": len(self._trees),
                "healthy": all(repo.root is not None for repo in self._trees.values())
            }
        }
        
        # 检查是否有不健康的组件
        if not all(status["components"].values()):
            status["status"] = "degraded"
            status["issues"] = [
                component for component, healthy in status["components"].items()
                if not healthy
            ]
        
        return status
    
    def save_to_file(self, filepath: str) -> bool:
        """
        保存系统状态到文件
        
        Args:
            filepath: 文件路径
            
        Returns:
            是否保存成功
        """
        try:
            data = {
                "system_info": self.get_system_info(),
                "trees": {},
                "metadata": self._tree_metadata
            }
            
            for tree_id, repo in self._trees.items():
                data["trees"][tree_id] = repo.to_dict(include_data=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"系统状态保存成功: {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"保存系统状态失败: {e}")
            return False
    
    def load_from_file(self, filepath: str) -> bool:
        """
        从文件加载系统状态
        
        Args:
            filepath: 文件路径
            
        Returns:
            是否加载成功
        """
        # TODO: 实现从文件加载系统状态
        self.logger.warning("从文件加载系统状态功能尚未实现")
        return False