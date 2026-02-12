"""
JSON文件存储实现
数据保存在JSON文件中，人类可读
"""
import json
import os
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from ..serializer import JSONSerializer
from .adapter import DataStoreAdapter
from ...exceptions import DataStoreError, TreeNotFoundError, NodeNotFoundError


class JSONStore(DataStoreAdapter):
    """JSON文件存储实现"""

    def __init__(self, filepath: str, serializer=None):
        """
        初始化JSON存储

        Args:
            filepath: JSON文件路径
            serializer: 序列化器，默认为JSONSerializer
        """
        self.filepath = Path(filepath)
        self.serializer = serializer or JSONSerializer()
        self._lock = threading.RLock()

        # 确保目录存在
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

        # 加载或初始化数据
        self._data = self._load_data()

    def _load_data(self) -> Dict:
        """从文件加载数据"""
        if not self.filepath.exists():
            return self._create_default_data()

        try:
            # 检查文件大小
            file_size = self.filepath.stat().st_size
            if file_size == 0:
                return self._create_default_data()

            with open(self.filepath, 'r', encoding='utf-8') as f:
                content = f.read().strip()

                # 检查是否为空内容
                if not content:
                    return self._create_default_data()

                json_data = json.loads(content)

            # 反序列化特殊类型
            return self.serializer.deserialize_from_dict(json_data)

        except json.JSONDecodeError as e:
            # 如果是JSON格式错误
            file_size = self.filepath.stat().st_size
            if file_size == 0:
                return self._create_default_data()
            else:
                # 备份损坏的文件
                backup_path = self.filepath.with_suffix('.json.bak')
                self.filepath.rename(backup_path)
                print(f"⚠ JSON文件损坏，已备份到: {backup_path}")
                return self._create_default_data()

        except Exception as e:
            raise DataStoreError(f"加载JSON文件失败: {e}")

    def _create_default_data(self) -> Dict:
        """创建默认数据结构"""
        return {
            'version': '1.0',
            'created_at': datetime.now(),
            'trees': {},
            'nodes': {},
            'node_data': {}
        }

    def _save_data(self):
        """保存数据到文件"""
        try:
            # 序列化数据（使用serializer处理datetime等特殊类型）
            serialized = self.serializer.serialize_to_dict(self._data)

            # 写入文件（原子操作）
            temp_file = self.filepath.with_suffix('.tmp')

            # 使用serializer的serialize方法，它会正确处理datetime
            data_bytes = self.serializer.serialize(serialized)

            with open(temp_file, 'wb') as f:
                f.write(data_bytes)

            # 替换原文件
            temp_file.replace(self.filepath)

        except Exception as e:
            raise DataStoreError(f"保存JSON文件失败: {e}")

    def save_tree(self, tree_data: Dict[str, Any]) -> bool:
        """保存树数据"""
        with self._lock:
            tree_id = tree_data.get('tree_id')
            if not tree_id:
                raise DataStoreError("树数据缺少tree_id")

            # 保存到数据结构
            if 'trees' not in self._data:
                self._data['trees'] = {}

            self._data['trees'][tree_id] = tree_data.copy()

            # 保存文件
            self._save_data()
            return True

    def load_tree(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """加载树数据"""
        with self._lock:
            if 'trees' not in self._data or tree_id not in self._data['trees']:
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            tree_data = self._data['trees'][tree_id].copy()

            # 加载所有节点
            if ('nodes' in self._data and
                    tree_id in self._data['nodes']):
                tree_data['nodes'] = list(self._data['nodes'][tree_id].values())

            return tree_data

    def delete_tree(self, tree_id: str) -> bool:
        """删除树"""
        with self._lock:
            if ('trees' not in self._data or
                    tree_id not in self._data['trees']):
                return False

            # 删除树
            del self._data['trees'][tree_id]

            # 删除所有节点
            if ('nodes' in self._data and
                    tree_id in self._data['nodes']):
                del self._data['nodes'][tree_id]

            # 删除所有节点数据
            if ('node_data' in self._data and
                    tree_id in self._data['node_data']):
                del self._data['node_data'][tree_id]

            # 保存文件
            self._save_data()
            return True

    def list_trees(self) -> List[Dict[str, Any]]:
        """列出所有树"""
        with self._lock:
            if 'trees' not in self._data:
                return []

            trees = []
            for tree_id, tree_data in self._data['trees'].items():
                tree_info = tree_data.copy()

                # 添加统计信息
                if ('nodes' in self._data and
                        tree_id in self._data['nodes']):
                    tree_info['node_count'] = len(self._data['nodes'][tree_id])
                else:
                    tree_info['node_count'] = 0

                trees.append(tree_info)

            return trees

    def save_node(self, tree_id: str, node_data: Dict[str, Any]) -> bool:
        """保存节点数据"""
        with self._lock:
            if ('trees' not in self._data or
                    tree_id not in self._data['trees']):
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            node_id = node_data.get('node_id')
            if not node_id:
                raise DataStoreError("节点数据缺少node_id")

            # 初始化数据结构
            if 'nodes' not in self._data:
                self._data['nodes'] = {}
            if tree_id not in self._data['nodes']:
                self._data['nodes'][tree_id] = {}

            # 保存节点
            self._data['nodes'][tree_id][node_id] = node_data.copy()

            # 保存文件
            self._save_data()
            return True

    def load_node(self, tree_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """加载节点数据"""
        with self._lock:
            if ('nodes' not in self._data or
                    tree_id not in self._data['nodes'] or
                    node_id not in self._data['nodes'][tree_id]):
                raise NodeNotFoundError(f"节点不存在: {node_id}")

            return self._data['nodes'][tree_id][node_id].copy()

    def load_all_nodes(self, tree_id: str) -> List[Dict[str, Any]]:
        """加载树的所有节点"""
        with self._lock:
            if ('nodes' not in self._data or
                    tree_id not in self._data['nodes']):
                return []

            return [node.copy() for node in self._data['nodes'][tree_id].values()]

    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """删除节点"""
        with self._lock:
            if ('nodes' not in self._data or
                    tree_id not in self._data['nodes'] or
                    node_id not in self._data['nodes'][tree_id]):
                return False

            # 删除节点
            del self._data['nodes'][tree_id][node_id]

            # 删除节点数据
            if ('node_data' in self._data and
                    tree_id in self._data['node_data'] and
                    node_id in self._data['node_data'][tree_id]):
                del self._data['node_data'][tree_id][node_id]

            # 保存文件
            self._save_data()
            return True

    def save_node_data(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            value: Any,
            timestamp: datetime
    ) -> bool:
        """保存节点维度数据"""
        with self._lock:
            # 检查树和节点
            if ('trees' not in self._data or
                    tree_id not in self._data['trees']):
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            if ('nodes' not in self._data or
                    tree_id not in self._data['nodes'] or
                    node_id not in self._data['nodes'][tree_id]):
                raise NodeNotFoundError(f"节点不存在: {node_id}")

            # 初始化数据结构
            if 'node_data' not in self._data:
                self._data['node_data'] = {}
            if tree_id not in self._data['node_data']:
                self._data['node_data'][tree_id] = {}
            if node_id not in self._data['node_data'][tree_id]:
                self._data['node_data'][tree_id][node_id] = {}
            if dimension not in self._data['node_data'][tree_id][node_id]:
                self._data['node_data'][tree_id][node_id][dimension] = []

            # 保存数据点
            data_point = {
                'value': value,
                'timestamp': timestamp,
                'dimension': dimension
            }

            self._data['node_data'][tree_id][node_id][dimension].append(data_point)

            # 按时间排序
            self._data['node_data'][tree_id][node_id][dimension].sort(
                key=lambda x: x['timestamp'],
                reverse=True
            )

            # 保存文件
            self._save_data()
            return True

    def load_node_data(
            self,
            tree_id: str,
            node_id: str,
            dimension: Optional[str] = None,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """加载节点维度数据"""
        with self._lock:
            if ('node_data' not in self._data or
                    tree_id not in self._data['node_data'] or
                    node_id not in self._data['node_data'][tree_id]):
                return {}

            node_dimensions = self._data['node_data'][tree_id][node_id]
            result = {}

            if dimension:
                if dimension in node_dimensions:
                    result[dimension] = self._filter_by_time(
                        node_dimensions[dimension],
                        start_time,
                        end_time
                    )
            else:
                for dim, data_points in node_dimensions.items():
                    filtered = self._filter_by_time(data_points, start_time, end_time)
                    if filtered:
                        result[dim] = filtered

            return result

    def _filter_by_time(
            self,
            data_points: List[Dict],
            start_time: Optional[datetime],
            end_time: Optional[datetime]
    ) -> List[Dict]:
        """按时间范围过滤"""
        if not data_points:
            return []

        filtered = []
        for point in data_points:
            timestamp = point['timestamp']

            if start_time and timestamp < start_time:
                continue
            if end_time and timestamp > end_time:
                continue

            filtered.append(point)

        return filtered

    def exists_tree(self, tree_id: str) -> bool:
        """检查树是否存在"""
        with self._lock:
            return ('trees' in self._data and
                    tree_id in self._data['trees'])

    def exists_node(self, tree_id: str, node_id: str) -> bool:
        """检查节点是否存在"""
        with self._lock:
            return ('nodes' in self._data and
                    tree_id in self._data['nodes'] and
                    node_id in self._data['nodes'][tree_id])

    def get_tree_stats(self, tree_id: str) -> Dict[str, Any]:
        """获取树统计信息"""
        with self._lock:
            if ('trees' not in self._data or
                    tree_id not in self._data['trees']):
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            stats = {
                'tree_id': tree_id,
                'node_count': 0,
                'dimension_count': 0,
                'data_point_count': 0
            }

            # 节点数
            if ('nodes' in self._data and
                    tree_id in self._data['nodes']):
                stats['node_count'] = len(self._data['nodes'][tree_id])

            # 维度数据点
            if ('node_data' in self._data and
                    tree_id in self._data['node_data']):
                for node_id, dimensions in self._data['node_data'][tree_id].items():
                    for dimension, points in dimensions.items():
                        stats['dimension_count'] += 1
                        stats['data_point_count'] += len(points)

            return stats

    def close(self):
        """关闭存储连接"""
        # JSON存储自动保存，无需特殊关闭
        pass

    def clear(self):
        """清空所有数据"""
        with self._lock:
            self._data = {
                'version': '1.0',
                'created_at': datetime.now(),
                'trees': {},
                'nodes': {},
                'node_data': {}
            }
            self._save_data()

    def backup(self, backup_path: str):
        """创建备份"""
        with self._lock:
            import shutil
            shutil.copy2(self.filepath, backup_path)

    def __str__(self):
        """字符串表示"""
        tree_count = len(self._data.get('trees', {}))
        node_count = sum(len(nodes) for nodes in self._data.get('nodes', {}).values())

        return (f"JSONStore(file={self.filepath}, trees={tree_count}, "
                f"nodes={node_count})")