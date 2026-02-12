"""
内存存储实现
将所有数据存储在内存字典中，程序结束即消失
适用于测试、缓存、临时计算
"""

from typing import Any, Optional, List, Tuple, Dict
from datetime import datetime
from .adapter import DataStoreAdapter, TimePointMetadata


class MemoryStore(DataStoreAdapter):
    """内存存储 - 所有数据存在字典里"""

    def __init__(self):
        """初始化内存存储"""
        # 数据结构：
        # self._data[tree_id][node_id][dimension][timestamp] = (value, metadata)
        self._data: Dict[str, Dict[str, Dict[str, Dict[str, Tuple[Any, Dict]]]]] = {}

        # 树结构数据（兼容老接口）
        self._trees: Dict[str, Dict] = {}
        self._nodes: Dict[str, Dict[str, Dict]] = {}

    # ========== 原有接口实现（保持不变） ==========

    def save_tree(self, tree_id: str, tree_data: Dict[str, Any]) -> None:
        """保存整棵树的结构数据"""
        self._trees[tree_id] = tree_data

    def load_tree(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """加载整棵树的结构数据"""
        return self._trees.get(tree_id)

    def save_node(self, tree_id: str, node_id: str, node_data: Dict[str, Any]) -> None:
        """保存单个节点的数据"""
        if tree_id not in self._nodes:
            self._nodes[tree_id] = {}
        self._nodes[tree_id][node_id] = node_data

    def load_node(self, tree_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """加载单个节点的数据"""
        return self._nodes.get(tree_id, {}).get(node_id)

    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """删除节点"""
        if tree_id in self._nodes and node_id in self._nodes[tree_id]:
            del self._nodes[tree_id][node_id]
            return True
        return False

    # ========== 新增接口实现：时间点存取 ==========

    def save_time_point(
        self,
        tree_id: str,
        node_id: str,
        dimension: str,
        timestamp: datetime,
        value: Any,
        quality: int = 1,
        unit: Optional[str] = None
    ) -> None:
        """保存单个时间点数据"""
        # 构建层级结构
        if tree_id not in self._data:
            self._data[tree_id] = {}
        if node_id not in self._data[tree_id]:
            self._data[tree_id][node_id] = {}
        if dimension not in self._data[tree_id][node_id]:
            self._data[tree_id][node_id][dimension] = {}

        # 时间戳转字符串作为key（保证JSON兼容）
        ts_key = timestamp.isoformat()

        # 构建元数据
        metadata = TimePointMetadata(quality=quality, unit=unit).to_dict()

        # 存储
        self._data[tree_id][node_id][dimension][ts_key] = (value, metadata)

    def get_time_points(
        self,
        tree_id: str,
        node_id: str,
        dimension: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Tuple[datetime, Any, Dict]]:
        """获取时间范围内的所有时间点"""
        result = []

        # 检查数据是否存在
        if (tree_id not in self._data or
            node_id not in self._data[tree_id] or
            dimension not in self._data[tree_id][node_id]):
            return result

        # 遍历该维度的所有时间点
        for ts_key, (value, metadata) in self._data[tree_id][node_id][dimension].items():
            try:
                timestamp = datetime.fromisoformat(ts_key)

                # 时间范围过滤
                if start_time and timestamp < start_time:
                    continue
                if end_time and timestamp > end_time:
                    continue

                result.append((timestamp, value, metadata))
            except ValueError:
                continue  # 跳过格式错误的时间戳

        # 按时间排序（升序）
        result.sort(key=lambda x: x[0])

        # 限制数量
        if limit and limit > 0:
            result = result[:limit]

        return result

    def get_latest_time_point(
        self,
        tree_id: str,
        node_id: str,
        dimension: str,
        before_time: Optional[datetime] = None
    ) -> Optional[Tuple[datetime, Any, Dict]]:
        """获取最新的时间点"""
        points = self.get_time_points(tree_id, node_id, dimension, end_time=before_time)
        return points[-1] if points else None

    def delete_time_points(
        self,
        tree_id: str,
        node_id: str,
        dimension: str,
        before_time: Optional[datetime] = None
    ) -> int:
        """删除时间点"""
        deleted_count = 0

        if (tree_id not in self._data or
            node_id not in self._data[tree_id] or
            dimension not in self._data[tree_id][node_id]):
            return 0

        # 找到要删除的key
        to_delete = []
        for ts_key in self._data[tree_id][node_id][dimension].keys():
            try:
                timestamp = datetime.fromisoformat(ts_key)
                if before_time is None or timestamp < before_time:
                    to_delete.append(ts_key)
            except ValueError:
                continue

        # 执行删除
        for ts_key in to_delete:
            del self._data[tree_id][node_id][dimension][ts_key]
            deleted_count += 1

        return deleted_count

    def delete_tree(self, tree_id: str) -> bool:
        """删除整棵树"""
        deleted = False

        # 删除树结构
        if tree_id in self._trees:
            del self._trees[tree_id]
            deleted = True

        # 删除节点数据
        if tree_id in self._nodes:
            del self._nodes[tree_id]
            deleted = True

        # 删除时间点数据
        if tree_id in self._data:
            del self._data[tree_id]
            deleted = True

        return deleted
    def get_dimensions(
        self,
        tree_id: str,
        node_id: Optional[str] = None
    ) -> List[str]:
        """获取所有出现过维度名称"""
        dimensions = set()

        if tree_id not in self._data:
            return []

        if node_id:
            # 只查特定节点
            if node_id in self._data[tree_id]:
                dimensions.update(self._data[tree_id][node_id].keys())
        else:
            # 查整棵树所有节点
            for node_data in self._data[tree_id].values():
                dimensions.update(node_data.keys())

        return sorted(list(dimensions))

    def get_time_range(
        self,
        tree_id: str,
        node_id: str,
        dimension: str
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """获取某个维度数据的时间范围"""
        points = self.get_time_points(tree_id, node_id, dimension)

        if not points:
            return None, None

        timestamps = [p[0] for p in points]
        return min(timestamps), max(timestamps)

    # ========== 工具方法 ==========

    def clear(self):
        """清空所有数据（用于测试）"""
        self._data.clear()
        self._trees.clear()
        self._nodes.clear()

    def get_stats(self) -> Dict:
        """获取存储统计信息"""
        tree_count = len(self._data)
        node_count = sum(len(t) for t in self._data.values())
        point_count = 0

        for tree in self._data.values():
            for node in tree.values():
                for dim in node.values():
                    point_count += len(dim)

        return {
            'trees': tree_count,
            'nodes': node_count,
            'time_points': point_count,
            'memory_estimate': f"~{point_count * 200} bytes"
        }
