"""
燃气输差分析系统主入口
集成IP模块、节点模块、维度模块、存储模块，提供完整的管理接口
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

from .exceptions import (
    TreeError, TreeNotFoundError, NodeError,
    ValidationError, ConfigError, DimensionNotFoundError
)
from .config.settings import SystemSettings
from .config.validator import ConfigValidator

from .core.ip import IncrementalIPProvider, IPAddress
from .core.node import NodeFactory, NodeRepository, TreeNode
from .data.dimensions import DimensionRegistry
from .data.storage.adapter import DataStoreAdapter
from .data.storage.memory_store import MemoryStore
from .core.time.timeline import Timeline
from .core.time.snapshot import SnapshotSystem


class TemporalTreeSystem:
    """
    燃气输差分析系统主类
    集成所有模块，提供完整的管理接口
    """

    def __init__(
            self,
            config: Optional[Dict[str, Any]] = None,
            storage: Optional[DataStoreAdapter] = None
    ):
        """
        初始化系统

        Args:
            config: 系统配置字典
            storage: 存储适配器（默认使用 MemoryStore）
        """
        # 加载配置
        self.settings = SystemSettings.from_dict(config) if config else SystemSettings()
        self.validator = ConfigValidator()

        # 初始化日志
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        # 存储适配器
        self._storage = storage or MemoryStore()
        self.logger.info(f"使用存储引擎: {self._storage.__class__.__name__}")

        # 核心组件（延迟初始化）
        self._ip_provider: Optional[IncrementalIPProvider] = None
        self._node_factory: Optional[NodeFactory] = None
        self._dimension_registry: Optional[DimensionRegistry] = None
        self._snapshot_system: Optional[SnapshotSystem] = None

        # 数据容器
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
            handlers=[logging.StreamHandler()]
        )

    def initialize(self) -> 'TemporalTreeSystem':
        """初始化系统组件"""
        if self._initialized:
            return self

        try:
            # 初始化IP提供者
            self._ip_provider = IncrementalIPProvider(
                max_depth=self.settings.max_tree_depth,
                max_children_per_node=self.settings.max_children_per_node
            )
            self.logger.debug(f"IP提供者初始化完成: 最大深度={self.settings.max_tree_depth}")

            # 初始化节点工厂
            self._node_factory = NodeFactory(self._ip_provider)
            self.logger.debug("节点工厂初始化完成")

            # 初始化维度注册表
            self._dimension_registry = DimensionRegistry()
            self.logger.debug(f"维度注册表初始化完成: {len(self._dimension_registry.list_dimensions())}个维度")

            # 初始化快照系统
            self._snapshot_system = SnapshotSystem()
            self.logger.debug("快照系统初始化完成")

            self._initialized = True
            self.logger.info("系统组件初始化完成")

        except Exception as e:
            self.logger.error(f"系统初始化失败: {e}")
            raise

        return self

    # ========== 树管理 ==========

    def create_tree(
            self,
            tree_id: str,
            name: str,
            description: str = "",
            metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        创建新的分析树

        Args:
            tree_id: 树ID（唯一标识）
            name: 树名称
            description: 树描述
            metadata: 额外元数据

        Returns:
            创建结果
        """
        if not self._initialized:
            self.initialize()

        if tree_id in self._trees:
            raise TreeError(f"树已存在: {tree_id}")

        try:
            # 准备根节点元数据
            root_metadata = metadata.copy() if metadata else {}
            root_metadata.update({
                "tree_id": tree_id,
                "description": description,
                "storage": self._storage,
                "tree_id_for_storage": tree_id
            })
            if "ip_address" in root_metadata:
                del root_metadata["ip_address"]
            # 创建根节点
            root_node = self._node_factory.create_root_node(name, root_metadata)

            # 创建节点仓库
            repository = NodeRepository(root_node)

            # 保存树
            self._trees[tree_id] = repository
            self._tree_metadata[tree_id] = {
                "id": tree_id,
                "name": name,
                "description": description,
                "created_at": datetime.now().isoformat(),
                "root_node_id": root_node.node_id,
                "node_count": 1,
                "metadata": root_metadata
            }

            # 持久化树结构
            self._save_tree_structure(tree_id, repository)

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

    def _save_tree_structure(self, tree_id: str, repository: NodeRepository):
        """持久化树结构"""
        try:
            # ✅ 1. 先保存树本身（创建trees表记录）
            tree_data = {
                "tree_id": tree_id,
                "name": self._tree_metadata[tree_id]["name"],
                "description": self._tree_metadata[tree_id].get("description", ""),
                "created_at": self._tree_metadata[tree_id]["created_at"]
            }
            self._storage.save_tree(tree_id, tree_data)

            # ✅ 2. 再保存根节点
            root = repository.root
            self._storage.save_node(tree_id, root.node_id, root.to_dict())

            # ✅ 3. 递归保存所有节点
            for node in repository.get_all_nodes():
                if node.node_id != root.node_id:  # 根节点已保存
                    self._storage.save_node(tree_id, node.node_id, node.to_dict())

        except Exception as e:
            self.logger.warning(f"树结构持久化失败: {e}")

    def get_tree(self, tree_id: str) -> NodeRepository:
        """获取树仓库"""
        if tree_id not in self._trees:
            raise TreeNotFoundError(tree_id=tree_id)
        return self._trees[tree_id]

    def delete_tree(self, tree_id: str) -> Dict[str, Any]:
        """删除树"""
        if tree_id not in self._trees:
            raise TreeNotFoundError(tree_id=tree_id)

        # 删除树
        del self._trees[tree_id]
        del self._tree_metadata[tree_id]

        # 从存储中删除
        try:
            self._storage.delete_tree(tree_id)
        except Exception as e:
            self.logger.warning(f"树持久化数据删除失败: {e}")

        self.logger.info(f"删除树成功: {tree_id}")

        return {
            "success": True,
            "tree_id": tree_id,
            "deleted_at": datetime.now().isoformat()
        }

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
                "node_count": len(repo.get_all_nodes()),
                "tree_depth": repo.get_tree_depth(),
                "root_node": metadata.get("root_node_id")
            })
        return trees

    # ========== 节点管理 ==========

    def add_node(
            self,
            tree_id: str,
            parent_node_id: str,
            name: str,
            metadata: Optional[Dict] = None
        ) -> Dict[str, Any]:
            """添加节点到树"""
            repository = self.get_tree(tree_id)
            parent = repository.get_node(parent_node_id)

            if not parent:
                raise NodeError(f"父节点不存在: {parent_node_id}")

            try:
                # 准备节点元数据
                node_metadata = metadata.copy() if metadata else {}
                node_metadata.update({
                    "tree_id": tree_id,
                    "storage": self._storage,
                    "tree_id_for_storage": tree_id
                })

                # 创建子节点
                node = self._node_factory.create_child_node(parent, name, node_metadata)

                # 添加到仓库
                repository.add_node(node)

                # ✅ 持久化 - 不需要再检查，因为树已经存在
                self._storage.save_node(tree_id, node.node_id, node.to_dict())

                # 更新树元数据
                self._tree_metadata[tree_id]["node_count"] = len(repository.get_all_nodes())

                self.logger.info(f"添加节点成功: {name} 到树 {tree_id}")

                return {
                    "success": True,
                    "node_id": node.node_id,
                    "name": node.name,
                    "ip": str(node.ip),
                    "level": node.level,
                    "parent_id": parent_node_id,
                    "tree_id": tree_id
                }

            except Exception as e:
                self.logger.error(f"添加节点失败: {name}, 错误: {e}")
                raise

    def get_node(self, tree_id: str, node_id: str) -> Optional[TreeNode]:
        """获取节点"""
        repository = self.get_tree(tree_id)
        return repository.get_node(node_id)

    def delete_node(self, tree_id: str, node_id: str) -> Dict[str, Any]:
        """删除节点"""
        repository = self.get_tree(tree_id)
        node = repository.get_node(node_id)

        if not node:
            raise NodeError(f"节点不存在: {node_id}")

        # 软删除
        node.delete()

        # 从仓库移除
        if node.parent:
            node.parent.remove_child(node)

        repository.remove_node(node_id)

        # 更新持久化
        self._storage.delete_node(tree_id, node_id)

        return {
            "success": True,
            "node_id": node_id,
            "deleted_at": node.deleted_at.isoformat()
        }

    # ========== 数据管理 ==========

    def set_node_data(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            value: Any,
            timestamp: Optional[datetime] = None,
            quality: int = 1,
            unit: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        设置节点维度数据

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            value: 数据值
            timestamp: 时间戳
            quality: 质量码
            unit: 单位

        Returns:
            设置结果
        """
        node = self.get_node(tree_id, node_id)
        if not node:
            raise NodeError(f"节点不存在: {node_id}")

        ts = timestamp or datetime.now()

        try:
            node.set_data(
                dimension=dimension,
                value=value,
                timestamp=ts,
                quality=quality,
                unit=unit,
                auto_persist=True  # Timeline会自动持久化
            )

            return {
                "success": True,
                "node_id": node_id,
                "dimension": dimension,
                "value": value,
                "timestamp": ts.isoformat()
            }

        except Exception as e:
            self.logger.error(f"设置节点数据失败: {node_id}.{dimension}, 错误: {e}")
            raise

    def get_node_data(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            timestamp: Optional[datetime] = None,
            tolerance: Optional[int] = None
    ) -> Any:
        """获取节点维度数据"""
        node = self.get_node(tree_id, node_id)
        if not node:
            raise NodeError(f"节点不存在: {node_id}")

        return node.get_data(dimension, timestamp, tolerance)

    def get_node_time_series(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None,
            limit: Optional[int] = None
    ) -> List[tuple]:
        """获取节点时间序列数据"""
        node = self.get_node(tree_id, node_id)
        if not node:
            raise NodeError(f"节点不存在: {node_id}")

        return node.get_time_series(dimension, start_time, end_time, limit)

    # ========== 维度计算 ==========

    def calculate_dimension(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            timestamp: Optional[datetime] = None
    ) -> Any:
        """
        计算维度值（用于计算型维度如输差率）

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            timestamp: 时间戳

        Returns:
            计算值
        """
        node = self.get_node(tree_id, node_id)
        if not node:
            raise NodeError(f"节点不存在: {node_id}")

        if not self._dimension_registry.has_dimension(dimension):
            raise DimensionNotFoundError(dimension_name=dimension)

        dim_obj = self._dimension_registry.get_dimension(dimension)

        if not dim_obj.is_calculated:
            # 非计算维度，直接返回存储值
            return node.get_data(dimension, timestamp)

        # 计算维度
        if dimension == "loss_rate":
            standard = node.get_data("standard_gas", timestamp) or 0
            meter = node.get_data("meter_gas", timestamp) or 0
            return dim_obj.calculate(standard, meter)

        # 其他计算维度可以扩展
        raise ValueError(f"不支持的计算维度: {dimension}")

    # ========== 输差分析 ==========

    def analyze_loss_rate(
            self,
            tree_id: str,
            threshold: float = 5.0,
            timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        分析树中所有节点的输差率

        Args:
            tree_id: 树ID
            threshold: 输差率阈值（百分比）
            timestamp: 时间点，None表示最新数据

        Returns:
            分析结果
        """
        repository = self.get_tree(tree_id)

        total_standard = 0.0
        total_meter = 0.0
        node_count = 0
        high_loss_nodes = []

        for node in repository.get_all_nodes():
            # 获取数据
            standard = node.get_data("standard_gas", timestamp) or 0.0
            meter = node.get_data("meter_gas", timestamp) or 0.0

            if standard > 0:
                total_standard += standard
                total_meter += meter
                node_count += 1

            # 计算输差率
            loss_rate = self.calculate_dimension(tree_id, node.node_id, "loss_rate", timestamp)

            if loss_rate > threshold:
                high_loss_nodes.append({
                    "node_id": node.node_id,
                    "name": node.name,
                    "ip": str(node.ip),
                    "loss_rate": loss_rate,
                    "loss_rate_percent": f"{loss_rate:.2f}%",
                    "standard_gas": standard,
                    "meter_gas": meter
                })

        # 总体输差率
        overall_loss_rate = 0.0
        if total_standard > 0:
            overall_loss_rate = ((total_standard - total_meter) / total_standard) * 100

        return {
            "tree_id": tree_id,
            "analysis_time": datetime.now().isoformat(),
            "timestamp": timestamp.isoformat() if timestamp else "latest",
            "threshold": threshold,
            "overall": {
                "total_standard_gas": total_standard,
                "total_meter_gas": total_meter,
                "loss_rate": overall_loss_rate,
                "loss_rate_percent": f"{overall_loss_rate:.2f}%",
                "node_count": node_count
            },
            "high_loss_nodes": high_loss_nodes,
            "high_loss_count": len(high_loss_nodes)
        }

    # ========== 快照管理 ==========

    def create_snapshot(
            self,
            tree_id: str,
            node_id: Optional[str] = None,
            metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """创建快照"""
        if not self._snapshot_system:
            from temporal_tree.core.time.snapshot import SnapshotSystem
            self._snapshot_system = SnapshotSystem()

        if node_id:
            node = self.get_node(tree_id, node_id)
            snapshot = self._snapshot_system.create_node_snapshot(node, metadata=metadata)
        else:
            tree = self.get_tree(tree_id)
            snapshot = self._snapshot_system.create_tree_snapshot(tree.root, metadata=metadata)

        return {
            "success": True,
            "snapshot_id": snapshot.snapshot_id,
            "timestamp": snapshot.timestamp.isoformat(),
            "metadata": snapshot.metadata
        }

    def restore_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """恢复快照"""
        if not self._snapshot_system:
            raise RuntimeError("快照系统未初始化")

        node = self._snapshot_system.restore_snapshot(snapshot_id)

        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "node": node.to_dict()
        }

    # ========== 持久化 ==========

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
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)

            self.logger.info(f"系统状态保存成功: {filepath}")
            return True

        except Exception as e:
            self.logger.error(f"保存系统状态失败: {e}")
            return False

    # ========== 系统状态 ==========

    def get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        total_nodes = sum(len(repo.get_all_nodes()) for repo in self._trees.values())

        return {
            "system_name": "燃气输差分析系统",
            "version": "1.0.0",
            "start_time": self._start_time.isoformat(),
            "uptime": str(datetime.now() - self._start_time),
            "initialized": self._initialized,
            "tree_count": len(self._trees),
            "total_nodes": total_nodes,
            "storage": self._storage.__class__.__name__,
            "dimensions": self._dimension_registry.list_dimensions_info() if self._dimension_registry else [],
            "settings": {
                "max_tree_depth": self.settings.max_tree_depth,
                "max_children_per_node": self.settings.max_children_per_node,
                "log_level": self.settings.log_level
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
                "snapshot_system": self._snapshot_system is not None,
                "storage": self._storage is not None,
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
                if not healthy and component != "initialized"  # initialized 是整体状态
            ]

        return status

    def __repr__(self) -> str:
        return f"TemporalTreeSystem(trees={len(self._trees)}, storage={self._storage.__class__.__name__})"