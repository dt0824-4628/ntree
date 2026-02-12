"""
快照系统
为节点或树创建状态快照
"""
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import hashlib
import json
import uuid

from ...exceptions import TimeError
from ...core.node.entity import TreeNode
from ...core.node.repository import NodeRepository
from .timeline import Timeline


class DateTimeEncoder(json.JSONEncoder):
    """自定义JSON编码器，支持datetime序列化"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class NodeSnapshot:
    """节点快照"""
    def __init__(self, snapshot_id: str, node_id: str, node_state: Dict,
                 timestamp: datetime, metadata: Optional[Dict] = None):
        self.snapshot_id = snapshot_id
        self.node_id = node_id
        self.node_state = node_state
        self.timestamp = timestamp
        self.metadata = metadata or {}


class TreeSnapshot:
    """树快照"""
    def __init__(self, snapshot_id: str, tree_id: str, tree_state: Dict,
                 timestamp: datetime, metadata: Optional[Dict] = None):
        self.snapshot_id = snapshot_id
        self.tree_id = tree_id
        self.tree_state = tree_state
        self.timestamp = timestamp
        self.metadata = metadata or {}


class SnapshotSystem:
    """快照系统 - 管理对象状态快照"""

    def __init__(self):
        self._timelines: Dict[str, Timeline] = {}  # object_id -> Timeline
        self._snapshots: Dict[str, Union[NodeSnapshot, TreeSnapshot]] = {}  # snapshot_id -> Snapshot

    def _generate_snapshot_id(self) -> str:
        """生成唯一的快照ID"""
        return f"snap_{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8]}"

    def create_node_snapshot(
        self,
        node: 'TreeNode',
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict] = None
    ) -> NodeSnapshot:
        """创建节点快照"""
        snapshot_id = self._generate_snapshot_id()
        ts = timestamp or datetime.now()

        # 获取节点状态
        node_state = node.to_dict()

        # 创建快照对象
        snapshot = NodeSnapshot(
            snapshot_id=snapshot_id,
            node_id=node.node_id,
            node_state=node_state,
            timestamp=ts,
            metadata=metadata or {}
        )

        # 保存快照
        self._snapshots[snapshot_id] = snapshot

        return snapshot

    def create_tree_snapshot(
        self,
        root_node: 'TreeNode',
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict] = None
    ) -> TreeSnapshot:
        """
        创建整棵树快照

        Args:
            root_node: 树的根节点
            timestamp: 快照时间
            metadata: 元数据

        Returns:
            树快照对象
        """
        snapshot_id = self._generate_snapshot_id()
        ts = timestamp or datetime.now()

        # 获取树状态 - 递归获取所有节点
        tree_state = {
            'root_node': root_node.to_dict(),
            'all_nodes': [n.to_dict() for n in root_node.get_descendants()]
        }

        # 创建快照对象
        snapshot = TreeSnapshot(
            snapshot_id=snapshot_id,
            tree_id=f"tree_{root_node.node_id}",
            tree_state=tree_state,
            timestamp=ts,
            metadata=metadata or {}
        )

        # 保存快照
        self._snapshots[snapshot_id] = snapshot

        return snapshot

    def restore_node_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """
        恢复节点快照

        Args:
            snapshot_id: 快照ID

        Returns:
            节点状态字典，如果找不到返回None
        """
        snapshot = self._snapshots.get(snapshot_id)
        if snapshot and isinstance(snapshot, NodeSnapshot):
            return snapshot.node_state
        return None

    def restore_tree_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """
        恢复树快照

        Args:
            snapshot_id: 快照ID

        Returns:
            树状态字典，如果找不到返回None
        """
        snapshot = self._snapshots.get(snapshot_id)
        if snapshot and isinstance(snapshot, TreeSnapshot):
            return snapshot.tree_state
        return None

    def get_node_snapshots(self, node_id: str) -> List[NodeSnapshot]:
        """
        获取节点的所有快照

        Args:
            node_id: 节点ID

        Returns:
            快照列表
        """
        snapshots = []
        for snapshot in self._snapshots.values():
            if isinstance(snapshot, NodeSnapshot) and snapshot.node_id == node_id:
                snapshots.append(snapshot)

        # 按时间倒序排序
        snapshots.sort(key=lambda x: x.timestamp, reverse=True)
        return snapshots

    def get_tree_snapshots(self, tree_id: str) -> List[TreeSnapshot]:
        """
        获取树的所有快照

        Args:
            tree_id: 树ID

        Returns:
            快照列表
        """
        snapshots = []
        for snapshot in self._snapshots.values():
            if isinstance(snapshot, TreeSnapshot) and snapshot.tree_id == tree_id:
                snapshots.append(snapshot)

        # 按时间倒序排序
        snapshots.sort(key=lambda x: x.timestamp, reverse=True)
        return snapshots

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """删除快照"""
        if snapshot_id in self._snapshots:
            del self._snapshots[snapshot_id]
            return True
        return False

    def clear(self):
        """清空所有快照"""
        self._snapshots.clear()
        self._timelines.clear()