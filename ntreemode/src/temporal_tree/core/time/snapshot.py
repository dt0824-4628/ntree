"""
快照系统
为节点或树创建状态快照
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
import hashlib
import json

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


class SnapshotSystem:
    """快照系统 - 管理对象状态快照"""

    def __init__(self):
        self._timelines: Dict[str, Timeline] = {}  # object_id -> Timeline

    def create_node_snapshot(self, node: TreeNode,
                            timestamp: Optional[datetime] = None,
                            comment: str = "") -> str:
        """
        创建节点快照

        Args:
            node: 节点对象
            timestamp: 快照时间，None表示当前时间
            comment: 快照注释

        Returns:
            快照ID（哈希值）
        """
        if timestamp is None:
            timestamp = datetime.now()

        # 获取节点状态
        node_state = node.to_dict(include_children=False, include_data=True)

        # 计算快照ID（基于内容和时间）
        snapshot_data = {
            'node_id': node.node_id,
            'timestamp': timestamp,
            'state': node_state
        }

        snapshot_json = json.dumps(snapshot_data, sort_keys=True, cls=DateTimeEncoder)
        snapshot_id = hashlib.md5(snapshot_json.encode()).hexdigest()[:16]

        # 获取或创建时间线
        timeline = self._get_timeline(node.node_id, 'node')

        # 添加快照
        timeline.add_time_point(
            timestamp=timestamp,
            data={
                'snapshot_id': snapshot_id,
                'node_state': node_state,
                'type': 'snapshot'
            },
            metadata={
                'comment': comment,
                'created_by': 'snapshot_system'
            }
        )

        return snapshot_id

    def create_tree_snapshot(self, repository: NodeRepository,
                           timestamp: Optional[datetime] = None,
                           comment: str = "") -> str:
        """
        创建整棵树快照

        Args:
            repository: 节点仓库
            timestamp: 快照时间
            comment: 快照注释

        Returns:
            快照ID
        """
        if timestamp is None:
            timestamp = datetime.now()

        # 获取树状态
        tree_state = repository.to_dict(include_data=True)

        # 使用根节点ID作为对象ID
        object_id = f"tree_{repository.root.node_id if repository.root else 'unknown'}"

        # 计算快照ID
        snapshot_data = {
            'tree_state': tree_state,
            'timestamp': timestamp
        }

        snapshot_json = json.dumps(snapshot_data, sort_keys=True, cls=DateTimeEncoder)
        snapshot_id = hashlib.md5(snapshot_json.encode()).hexdigest()[:16]

        # 获取或创建时间线
        timeline = self._get_timeline(object_id, 'tree')

        # 添加快照
        timeline.add_time_point(
            timestamp=timestamp,
            data={
                'snapshot_id': snapshot_id,
                'tree_state': tree_state,
                'type': 'tree_snapshot'
            },
            metadata={
                'comment': comment,
                'node_count': repository.get_node_count(),
                'tree_depth': repository.get_tree_depth()
            }
        )

        return snapshot_id

    def restore_node_snapshot(self, node_id: str,
                            snapshot_id: Optional[str] = None,
                            timestamp: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        恢复节点快照

        Args:
            node_id: 节点ID
            snapshot_id: 快照ID，如果为None则使用时间戳
            timestamp: 时间戳，如果指定则恢复该时间点的状态

        Returns:
            节点状态字典，如果找不到返回None
        """
        timeline = self._timelines.get(f"node_{node_id}")
        if not timeline:
            return None

        time_point = None

        if snapshot_id:
            # 根据快照ID查找
            for tp in timeline.get_time_points():
                if tp.data.get('snapshot_id') == snapshot_id:
                    time_point = tp
                    break
        elif timestamp:
            # 根据时间戳查找
            time_point = timeline.get_nearest_time_point(timestamp)

        if time_point and 'node_state' in time_point.data:
            return time_point.data['node_state']

        return None

    def get_node_snapshots(self, node_id: str,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        获取节点快照列表

        Args:
            node_id: 节点ID
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            快照列表
        """
        timeline = self._timelines.get(f"node_{node_id}")
        if not timeline:
            return []

        time_points = timeline.get_time_range(start_time, end_time)

        snapshots = []
        for tp in time_points:
            if tp.data.get('type') == 'snapshot':
                snapshots.append({
                    'snapshot_id': tp.data.get('snapshot_id'),
                    'timestamp': tp.timestamp,
                    'comment': tp.metadata.get('comment', ''),
                    'data_summary': {
                        'dimensions': list(tp.data.get('node_state', {}).get('data', {}).keys())
                    }
                })

        return snapshots

    def _get_timeline(self, object_id: str, object_type: str) -> Timeline:
        """获取或创建时间线"""
        key = f"{object_type}_{object_id}"

        if key not in self._timelines:
            self._timelines[key] = Timeline(object_id, object_type)

        return self._timelines[key]

    def get_timeline(self, object_id: str, object_type: str = "node") -> Optional[Timeline]:
        """获取时间线"""
        key = f"{object_type}_{object_id}"
        return self._timelines.get(key)

    def delete_timeline(self, object_id: str, object_type: str = "node") -> bool:
        """删除时间线"""
        key = f"{object_type}_{object_id}"
        if key in self._timelines:
            del self._timelines[key]
            return True
        return False

    def save_all(self, base_path: str) -> bool:
        """保存所有时间线"""
        try:
            import os
            os.makedirs(base_path, exist_ok=True)

            for key, timeline in self._timelines.items():
                filepath = os.path.join(base_path, f"{key}.json")
                timeline.save_to_json(filepath)

            return True
        except Exception as e:
            raise TimeError(f"保存快照系统失败: {e}")

    def load_all(self, base_path: str) -> bool:
        """加载所有时间线"""
        try:
            import os
            import glob

            pattern = os.path.join(base_path, "*.json")
            for filepath in glob.glob(pattern):
                try:
                    timeline = Timeline.load_from_json(filepath)
                    key = f"{timeline.object_type}_{timeline.object_id}"
                    self._timelines[key] = timeline
                except Exception as e:
                    print(f"加载时间线失败 {filepath}: {e}")

            return True
        except Exception as e:
            raise TimeError(f"加载快照系统失败: {e}")