"""
JSON文件存储实现
将所有数据存储在JSON文件中，人类可读，轻量级
适用于小项目、原型开发、配置文件
"""

import json
import os
from typing import Any, Optional, List, Tuple, Dict
from datetime import datetime
from pathlib import Path

from .adapter import DataStoreAdapter, TimePointMetadata
from ...exceptions import StorageError


class DateTimeEncoder(json.JSONEncoder):
    """自定义JSON编码器，处理datetime对象"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Path):
            return str(obj)
        return super().default(obj)


class JSONStore(DataStoreAdapter):
    """JSON文件存储 - 所有数据存在单个JSON文件中"""

    def __init__(self, file_path: str):
        """
        初始化JSON存储

        Args:
            file_path: JSON文件路径
        """
        self.file_path = Path(file_path)
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        """确保JSON文件存在"""
        if not self.file_path.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            self._save_data({
                'trees': {},      # 树结构数据
                'nodes': {},      # 节点数据
                'time_series': {} # 时间序列数据
            })

    def _load_data(self) -> Dict:
        """加载JSON文件"""
        try:
            if self.file_path.exists():
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {'trees': {}, 'nodes': {}, 'time_series': {}}
        except json.JSONDecodeError as e:
            raise StorageError(f"JSON文件损坏: {e}")
        except Exception as e:
            raise StorageError(f"读取JSON文件失败: {e}")

    def _save_data(self, data: Dict):
        """保存JSON文件"""
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, cls=DateTimeEncoder, indent=2, ensure_ascii=False)
        except Exception as e:
            raise StorageError(f"写入JSON文件失败: {e}")

    # ========== 原有接口实现 ==========

    def save_tree(self, tree_id: str, tree_data: Dict[str, Any]) -> None:
        """保存整棵树的结构数据"""
        data = self._load_data()
        data['trees'][tree_id] = tree_data
        self._save_data(data)

    def load_tree(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """加载整棵树的结构数据"""
        data = self._load_data()
        return data['trees'].get(tree_id)

    def delete_tree(self, tree_id: str) -> bool:
        """删除整棵树"""
        data = self._load_data()
        deleted = False

        if tree_id in data['trees']:
            del data['trees'][tree_id]
            deleted = True

        # 删除该树下的所有节点数据
        if tree_id in data['nodes']:
            del data['nodes'][tree_id]
            deleted = True

        # 删除该树下的所有时间序列数据
        if tree_id in data['time_series']:
            del data['time_series'][tree_id]
            deleted = True

        if deleted:
            self._save_data(data)
        return deleted

    def save_node(self, tree_id: str, node_id: str, node_data: Dict[str, Any]) -> None:
        """保存单个节点的数据"""
        data = self._load_data()
        if tree_id not in data['nodes']:
            data['nodes'][tree_id] = {}
        data['nodes'][tree_id][node_id] = node_data
        self._save_data(data)

    def load_node(self, tree_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """加载单个节点的数据"""
        data = self._load_data()
        return data['nodes'].get(tree_id, {}).get(node_id)

    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """删除节点"""
        data = self._load_data()
        if tree_id in data['nodes'] and node_id in data['nodes'][tree_id]:
            del data['nodes'][tree_id][node_id]
            self._save_data(data)
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
        data = self._load_data()

        # 构建层级结构
        if tree_id not in data['time_series']:
            data['time_series'][tree_id] = {}
        if node_id not in data['time_series'][tree_id]:
            data['time_series'][tree_id][node_id] = {}
        if dimension not in data['time_series'][tree_id][node_id]:
            data['time_series'][tree_id][node_id][dimension] = {}

        # 时间戳转字符串
        ts_key = timestamp.isoformat()

        # 构建元数据
        metadata = TimePointMetadata(quality=quality, unit=unit).to_dict()

        # 存储
        data['time_series'][tree_id][node_id][dimension][ts_key] = {
            'value': value,
            'metadata': metadata
        }

        self._save_data(data)

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
        data = self._load_data()
        result = []

        # 检查数据是否存在
        try:
            points = data['time_series'][tree_id][node_id][dimension]
        except KeyError:
            return result

        # 遍历该维度的所有时间点
        for ts_key, point_data in points.items():
            try:
                timestamp = datetime.fromisoformat(ts_key)
                value = point_data['value']
                metadata = point_data.get('metadata', {})

                # 时间范围过滤
                if start_time and timestamp < start_time:
                    continue
                if end_time and timestamp > end_time:
                    continue

                result.append((timestamp, value, metadata))
            except (ValueError, KeyError):
                continue

        # 按时间排序
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
        data = self._load_data()
        deleted_count = 0

        try:
            points = data['time_series'][tree_id][node_id][dimension]
        except KeyError:
            return 0

        # 找到要删除的key
        to_delete = []
        for ts_key in points.keys():
            try:
                timestamp = datetime.fromisoformat(ts_key)
                if before_time is None or timestamp < before_time:
                    to_delete.append(ts_key)
            except ValueError:
                continue

        # 执行删除
        for ts_key in to_delete:
            del points[ts_key]
            deleted_count += 1

        # 如果维度下没有数据了，清理空结构
        if len(points) == 0:
            del data['time_series'][tree_id][node_id][dimension]
            if len(data['time_series'][tree_id][node_id]) == 0:
                del data['time_series'][tree_id][node_id]
                if len(data['time_series'][tree_id]) == 0:
                    del data['time_series'][tree_id]

        if deleted_count > 0:
            self._save_data(data)

        return deleted_count

    def get_dimensions(
        self,
        tree_id: str,
        node_id: Optional[str] = None
    ) -> List[str]:
        """获取所有出现过维度名称"""
        data = self._load_data()
        dimensions = set()

        if tree_id not in data['time_series']:
            return []

        if node_id:
            # 只查特定节点
            if node_id in data['time_series'][tree_id]:
                dimensions.update(data['time_series'][tree_id][node_id].keys())
        else:
            # 查整棵树所有节点
            for node_data in data['time_series'][tree_id].values():
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
        if self.file_path.exists():
            self.file_path.unlink()
        self._ensure_file_exists()

    def get_file_path(self) -> str:
        """获取文件路径"""
        return str(self.file_path)