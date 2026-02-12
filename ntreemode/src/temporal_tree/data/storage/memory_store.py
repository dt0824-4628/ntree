"""
内存存储实现
数据保存在内存中，程序结束即消失
"""
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict

from ..serializer import JSONSerializer
from .adapter import DataStoreAdapter
from ...exceptions import DataStoreError, TreeNotFoundError, NodeNotFoundError


class MemoryStore(DataStoreAdapter):
    """内存存储实现"""

    def __init__(self, serializer=None):
        """
        初始化内存存储

        Args:
            serializer: 序列化器，默认为JSONSerializer
        """
        self.serializer = serializer or JSONSerializer()
        self._lock = threading.RLock()  # 线程安全锁

        # 内存数据结构
        self._trees: Dict[str, Dict] = {}  # tree_id -> tree_data
        self._nodes: Dict[str, Dict[str, Dict]] = defaultdict(dict)  # tree_id -> {node_id -> node_data}
        self._node_data: Dict[str, Dict[str, Dict[str, List]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )  # tree_id -> node_id -> dimension -> [data_points]

        # 索引
        self._tree_by_name: Dict[str, str] = {}  # tree_name -> tree_id
        self._node_by_ip: Dict[str, Dict[str, str]] = defaultdict(dict)  # tree_id -> ip -> node_id

    def save_tree(self, tree_data: Dict[str, Any]) -> bool:
        """保存树数据"""
        with self._lock:
            tree_id = tree_data.get('tree_id')
            if not tree_id:
                raise DataStoreError("树数据缺少tree_id")

            # 保存树
            self._trees[tree_id] = tree_data.copy()

            # 建立名称索引
            tree_name = tree_data.get('name')
            if tree_name:
                self._tree_by_name[tree_name] = tree_id

            # 保存根节点
            root_node = tree_data.get('root_node')
            if root_node:
                self.save_node(tree_id, root_node)

            return True

    def load_tree(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """加载树数据"""
        with self._lock:
            if tree_id not in self._trees:
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            tree_data = self._trees[tree_id].copy()

            # 获取所有节点
            if tree_id in self._nodes:
                tree_data['nodes'] = list(self._nodes[tree_id].values())

            return tree_data

    def delete_tree(self, tree_id: str) -> bool:
        """删除树及其所有数据"""
        with self._lock:
            if tree_id not in self._trees:
                return False

            # 删除树
            tree_data = self._trees.pop(tree_id)

            # 删除名称索引
            tree_name = tree_data.get('name')
            if tree_name and tree_name in self._tree_by_name:
                self._tree_by_name.pop(tree_name, None)

            # 删除所有节点
            if tree_id in self._nodes:
                self._nodes.pop(tree_id, None)

            # 删除所有节点数据
            if tree_id in self._node_data:
                self._node_data.pop(tree_id, None)

            # 删除IP索引
            if tree_id in self._node_by_ip:
                self._node_by_ip.pop(tree_id, None)

            return True

    def list_trees(self) -> List[Dict[str, Any]]:
        """列出所有树"""
        with self._lock:
            trees = []
            for tree_id, tree_data in self._trees.items():
                tree_info = tree_data.copy()

                # 添加统计信息
                node_count = len(self._nodes.get(tree_id, {}))
                tree_info['node_count'] = node_count

                trees.append(tree_info)

            return trees

    def save_node(self, tree_id: str, node_data: Dict[str, Any]) -> bool:
        """保存节点数据"""
        with self._lock:
            if tree_id not in self._trees:
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            node_id = node_data.get('node_id')
            if not node_id:
                raise DataStoreError("节点数据缺少node_id")

            # 保存节点
            self._nodes[tree_id][node_id] = node_data.copy()

            # 建立IP索引
            ip_address = node_data.get('ip_address')
            if ip_address:
                self._node_by_ip[tree_id][ip_address] = node_id

            return True

    def load_node(self, tree_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """加载节点数据"""
        with self._lock:
            if tree_id not in self._nodes:
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            if node_id not in self._nodes[tree_id]:
                raise NodeNotFoundError(f"节点不存在: {node_id}")

            return self._nodes[tree_id][node_id].copy()

    def load_all_nodes(self, tree_id: str) -> List[Dict[str, Any]]:
        """加载树的所有节点"""
        with self._lock:
            if tree_id not in self._nodes:
                return []

            return [node.copy() for node in self._nodes[tree_id].values()]

    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """删除节点"""
        with self._lock:
            if tree_id not in self._nodes:
                return False

            if node_id not in self._nodes[tree_id]:
                return False

            # 删除IP索引
            node_data = self._nodes[tree_id][node_id]
            ip_address = node_data.get('ip_address')
            if ip_address and tree_id in self._node_by_ip:
                self._node_by_ip[tree_id].pop(ip_address, None)

            # 删除节点
            self._nodes[tree_id].pop(node_id, None)

            # 删除节点数据
            if (tree_id in self._node_data and
                    node_id in self._node_data[tree_id]):
                self._node_data[tree_id].pop(node_id, None)

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
            # 检查树和节点是否存在
            if tree_id not in self._trees:
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            if tree_id not in self._nodes or node_id not in self._nodes[tree_id]:
                raise NodeNotFoundError(f"节点不存在: {node_id}")

            # 保存数据点
            data_point = {
                'value': value,
                'timestamp': timestamp,
                'dimension': dimension
            }

            self._node_data[tree_id][node_id][dimension].append(data_point)

            # 按时间排序（最新的在前）
            self._node_data[tree_id][node_id][dimension].sort(
                key=lambda x: x['timestamp'],
                reverse=True
            )

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
            if (tree_id not in self._node_data or
                    node_id not in self._node_data[tree_id]):
                return {}

            node_dimensions = self._node_data[tree_id][node_id]
            result = {}

            if dimension:
                # 获取特定维度
                if dimension in node_dimensions:
                    data_points = self._filter_by_time(
                        node_dimensions[dimension],
                        start_time,
                        end_time
                    )
                    result[dimension] = data_points
            else:
                # 获取所有维度
                for dim, data_points in node_dimensions.items():
                    filtered_points = self._filter_by_time(
                        data_points,
                        start_time,
                        end_time
                    )
                    if filtered_points:
                        result[dim] = filtered_points

            return result

    def _filter_by_time(
            self,
            data_points: List[Dict],
            start_time: Optional[datetime],
            end_time: Optional[datetime]
    ) -> List[Dict]:
        """按时间范围过滤数据点"""
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
            return tree_id in self._trees

    def exists_node(self, tree_id: str, node_id: str) -> bool:
        """检查节点是否存在"""
        with self._lock:
            return (tree_id in self._nodes and
                    node_id in self._nodes[tree_id])

    def get_tree_stats(self, tree_id: str) -> Dict[str, Any]:
        """获取树统计信息"""
        with self._lock:
            if tree_id not in self._trees:
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            stats = {
                'tree_id': tree_id,
                'node_count': len(self._nodes.get(tree_id, {})),
                'dimension_count': 0,
                'data_point_count': 0
            }

            # 统计维度数据点
            if tree_id in self._node_data:
                for node_id, dimensions in self._node_data[tree_id].items():
                    for dimension, points in dimensions.items():
                        stats['dimension_count'] += 1
                        stats['data_point_count'] += len(points)

            return stats

    def get_node_by_ip(self, tree_id: str, ip_address: str) -> Optional[Dict[str, Any]]:
        """通过IP地址获取节点"""
        with self._lock:
            if (tree_id in self._node_by_ip and
                    ip_address in self._node_by_ip[tree_id]):
                node_id = self._node_by_ip[tree_id][ip_address]
                return self.load_node(tree_id, node_id)
            return None

    def close(self):
        """关闭存储连接（内存存储无操作）"""
        pass

    def clear(self):
        """清空所有数据（测试用）"""
        with self._lock:
            self._trees.clear()
            self._nodes.clear()
            self._node_data.clear()
            self._tree_by_name.clear()
            self._node_by_ip.clear()

    def __str__(self):
        """字符串表示"""
        tree_count = len(self._trees)
        node_count = sum(len(nodes) for nodes in self._nodes.values())

        return (f"MemoryStore(trees={tree_count}, nodes={node_count}, "
                f"data_points={self._count_data_points()})")

    def _count_data_points(self) -> int:
        """计算总数据点数"""
        count = 0
        for tree_id in self._node_data:
            for node_id in self._node_data[tree_id]:
                for dimension in self._node_data[tree_id][node_id]:
                    count += len(self._node_data[tree_id][node_id][dimension])
        return count
