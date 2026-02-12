"""
时间线管理
管理节点或树的时间序列状态
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import json

from ...exceptions import TimeError, InvalidTimestampError


@dataclass
class TimePoint:
    """时间点表示"""
    timestamp: datetime
    data: Dict[str, Any]
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        self.metadata.setdefault('created_at', datetime.now())

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'data': self.data,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimePoint':
        """从字典创建"""
        timestamp = datetime.fromisoformat(data['timestamp'])
        return cls(
            timestamp=timestamp,
            data=data['data'],
            metadata=data.get('metadata', {})
        )


class Timeline:
    """
    时间线类
    管理对象（节点或树）随时间变化的状态
    """

    def __init__(self, object_id: str, object_type: str = "node"):
        """
        初始化时间线

        Args:
            object_id: 对象ID（节点ID或树ID）
            object_type: 对象类型，'node' 或 'tree'
        """
        self.object_id = object_id
        self.object_type = object_type

        # 时间点存储：时间戳 -> TimePoint
        self._time_points: Dict[datetime, TimePoint] = {}

        # 元数据
        self.metadata = {
            'created_at': datetime.now(),
            'object_id': object_id,
            'object_type': object_type,
            'time_point_count': 0,
            'time_range': None
        }

    def add_time_point(self, timestamp: datetime, data: Dict[str, Any],
                       metadata: Optional[Dict[str, Any]] = None) -> TimePoint:
        """
        添加时间点

        Args:
            timestamp: 时间戳
            data: 时间点数据
            metadata: 元数据

        Returns:
            创建的时间点

        Raises:
            InvalidTimestampError: 时间戳无效
        """
        # 验证时间戳
        if not isinstance(timestamp, datetime):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except:
                raise InvalidTimestampError(str(timestamp))

        # 检查时间点是否已存在（允许覆盖）
        if timestamp in self._time_points:
            # 更新时间点
            time_point = self._time_points[timestamp]
            time_point.data.update(data)
            if metadata:
                time_point.metadata.update(metadata)
        else:
            # 创建新时间点
            time_point = TimePoint(
                timestamp=timestamp,
                data=data.copy(),
                metadata=metadata.copy() if metadata else {}
            )
            self._time_points[timestamp] = time_point

        # 更新时间线元数据
        self._update_metadata()

        return time_point

    def get_time_point(self, timestamp: datetime) -> Optional[TimePoint]:
        """
        获取指定时间点

        Args:
            timestamp: 时间戳

        Returns:
            时间点，如果不存在返回None
        """
        return self._time_points.get(timestamp)

    def get_nearest_time_point(self, timestamp: datetime,
                               max_delta: Optional[timedelta] = None) -> Optional[TimePoint]:
        """
        获取最接近的时间点

        Args:
            timestamp: 目标时间戳
            max_delta: 最大时间差，如果超过则返回None

        Returns:
            最接近的时间点
        """
        if not self._time_points:
            return None

        # 寻找最接近的时间点
        closest_time = None
        min_delta = None

        for time_key in self._time_points.keys():
            delta = abs(time_key - timestamp)

            if min_delta is None or delta < min_delta:
                min_delta = delta
                closest_time = time_key

        # 检查是否在允许的范围内
        if max_delta is not None and min_delta > max_delta:
            return None

        return self._time_points[closest_time] if closest_time else None

    def get_time_range(self, start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None) -> List[TimePoint]:
        """
        获取时间范围内的所有时间点

        Args:
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            时间点列表
        """
        result = []

        for timestamp, time_point in self._time_points.items():
            # 检查时间范围
            if start_time and timestamp < start_time:
                continue
            if end_time and timestamp > end_time:
                continue

            result.append(time_point)

        # 按时间排序
        result.sort(key=lambda tp: tp.timestamp)

        return result

    def get_latest(self) -> Optional[TimePoint]:
        """获取最新时间点"""
        if not self._time_points:
            return None

        latest_time = max(self._time_points.keys())
        return self._time_points[latest_time]

    def get_earliest(self) -> Optional[TimePoint]:
        """获取最早时间点"""
        if not self._time_points:
            return None

        earliest_time = min(self._time_points.keys())
        return self._time_points[earliest_time]

    def delete_time_point(self, timestamp: datetime) -> bool:
        """
        删除时间点

        Args:
            timestamp: 时间戳

        Returns:
            是否删除成功
        """
        if timestamp in self._time_points:
            del self._time_points[timestamp]
            self._update_metadata()
            return True
        return False

    def clear(self, before_time: Optional[datetime] = None) -> int:
        """
        清空时间点

        Args:
            before_time: 如果指定，只删除此时间之前的时间点

        Returns:
            删除的时间点数量
        """
        if before_time is None:
            count = len(self._time_points)
            self._time_points.clear()
        else:
            to_delete = [
                timestamp for timestamp in self._time_points.keys()
                if timestamp < before_time
            ]
            count = len(to_delete)
            for timestamp in to_delete:
                del self._time_points[timestamp]

        self._update_metadata()
        return count

    def get_time_points(self) -> List[TimePoint]:
        """获取所有时间点（按时间排序）"""
        points = list(self._time_points.values())
        points.sort(key=lambda tp: tp.timestamp)
        return points

    def get_timestamps(self) -> List[datetime]:
        """获取所有时间戳（排序后）"""
        timestamps = list(self._time_points.keys())
        timestamps.sort()
        return timestamps

    def has_time_point(self, timestamp: datetime) -> bool:
        """检查是否存在指定时间点"""
        return timestamp in self._time_points

    def get_time_series(self, key: str) -> List[Dict[str, Any]]:
        """
        获取指定键的时间序列数据

        Args:
            key: 数据键名

        Returns:
            时间序列数据：[{timestamp: ..., value: ...}, ...]
        """
        series = []

        for time_point in self.get_time_points():
            if key in time_point.data:
                series.append({
                    'timestamp': time_point.timestamp,
                    'value': time_point.data[key]
                })

        return series

    def _update_metadata(self):
        """更新元数据"""
        self.metadata['time_point_count'] = len(self._time_points)
        self.metadata['updated_at'] = datetime.now()

        # 更新时间范围
        if self._time_points:
            timestamps = self.get_timestamps()
            self.metadata['time_range'] = {
                'start': timestamps[0].isoformat(),
                'end': timestamps[-1].isoformat(),
                'duration_days': (timestamps[-1] - timestamps[0]).days
            }
        else:
            self.metadata['time_range'] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        time_points_dict = {}
        for timestamp, time_point in self._time_points.items():
            time_points_dict[timestamp.isoformat()] = time_point.to_dict()

        return {
            'object_id': self.object_id,
            'object_type': self.object_type,
            'metadata': self.metadata,
            'time_points': time_points_dict
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Timeline':
        """从字典创建"""
        timeline = cls(
            object_id=data['object_id'],
            object_type=data['object_type']
        )

        timeline.metadata = data['metadata']

        # 恢复时间点
        for time_str, tp_data in data['time_points'].items():
            timestamp = datetime.fromisoformat(time_str)
            time_point = TimePoint.from_dict(tp_data)
            timeline._time_points[timestamp] = time_point

        return timeline

    def save_to_json(self, filepath: str) -> bool:
        """保存到JSON文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            raise TimeError(f"保存时间线失败: {e}")

    @classmethod
    def load_from_json(cls, filepath: str) -> 'Timeline':
        """从JSON文件加载"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return cls.from_dict(data)
        except Exception as e:
            raise TimeError(f"加载时间线失败: {e}")

    def __len__(self) -> int:
        """获取时间点数量"""
        return len(self._time_points)

    def __contains__(self, timestamp: datetime) -> bool:
        """检查是否包含时间点"""
        return timestamp in self._time_points

    def __str__(self) -> str:
        return f"Timeline({self.object_id}, 时间点: {len(self)})"