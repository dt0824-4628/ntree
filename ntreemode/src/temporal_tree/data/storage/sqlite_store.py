"""
SQLite数据库存储实现
使用SQLite数据库持久化数据，支持索引查询、并发访问
适用于生产环境、大规模数据
"""

import sqlite3
import json
from typing import Any, Optional, List, Tuple, Dict
from datetime import datetime
from pathlib import Path

from .adapter import DataStoreAdapter, TimePointMetadata
from ...exceptions import StorageError


# 【修复】注册ISO格式的时间转换器
sqlite3.register_converter(
    "timestamp",
    lambda b: datetime.fromisoformat(b.decode())
)
sqlite3.register_adapter(
    datetime,
    lambda dt: dt.isoformat()
)


class SQLiteStore(DataStoreAdapter):
    """SQLite数据库存储"""

    def __init__(self, db_path: str):
        """
        初始化SQLite存储

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = Path(db_path)
        self._connection = None
        self._init_db()

    def _get_connection(self):
        """获取数据库连接"""
        if self._connection is None:
            try:
                # 确保目录存在
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                # 创建连接 - 启用自定义时间转换器
                self._connection = sqlite3.connect(
                    str(self.db_path),
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                    timeout=10
                )
                # 启用外键约束
                self._connection.execute("PRAGMA foreign_keys = ON")
                # 返回字典类型行
                self._connection.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                raise StorageError(f"连接数据库失败: {e}")
        return self._connection

    @property
    def conn(self):
        """获取连接（兼容原有代码）"""
        return self._get_connection()

    @property
    def cursor(self):
        """获取游标（兼容原有代码）"""
        return self.conn.cursor()

    def _init_db(self):
        """初始化数据库表结构"""
        conn = self._get_connection()
        cursor = conn.cursor()

        # 先临时禁用外键约束（建表时需要）
        cursor.execute("PRAGMA foreign_keys = OFF")

        # ===== 原有表结构 =====
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trees (
                tree_id TEXT PRIMARY KEY,
                tree_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                tree_id TEXT NOT NULL,
                node_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tree_id) REFERENCES trees(tree_id) ON DELETE CASCADE
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS node_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tree_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                data_key TEXT NOT NULL,
                data_value TEXT NOT NULL,
                timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tree_id) REFERENCES trees(tree_id) ON DELETE CASCADE,
                FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
            )
        ''')

        # ===== 新增表结构：时间序列数据 =====
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS time_series (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tree_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                dimension TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                value TEXT NOT NULL,
                quality INTEGER DEFAULT 1,
                unit TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tree_id) REFERENCES trees(tree_id) ON DELETE CASCADE,
                FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
            )
        ''')

        # 创建复合索引（加速查询）
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_time_series_query 
            ON time_series(tree_id, node_id, dimension, timestamp)
        ''')

        # 创建唯一约束（同一个时间点只能有一个值）
        cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_time_series_unique
            ON time_series(tree_id, node_id, dimension, timestamp)
        ''')

        # 维度统计表（缓存维度信息，加速查询）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dimension_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tree_id TEXT NOT NULL,
                node_id TEXT,
                dimension TEXT NOT NULL,
                min_time TIMESTAMP,
                max_time TIMESTAMP,
                count INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tree_id, node_id, dimension)
            )
        ''')

        # 重新启用外键约束
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.commit()

    # ========== 原有接口实现 ==========

    def save_tree(self, tree_id: str, tree_data: Dict[str, Any]) -> None:
        """保存整棵树的结构数据"""
        cursor = self.cursor
        cursor.execute(
            "INSERT OR REPLACE INTO trees (tree_id, tree_data) VALUES (?, ?)",
            (tree_id, json.dumps(tree_data, ensure_ascii=False))
        )
        self.conn.commit()

    def load_tree(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """加载整棵树的结构数据"""
        cursor = self.cursor
        cursor.execute(
            "SELECT tree_data FROM trees WHERE tree_id = ?",
            (tree_id,)
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None

    def delete_tree(self, tree_id: str) -> bool:
        """删除整棵树"""
        cursor = self.cursor
        cursor.execute("DELETE FROM trees WHERE tree_id = ?", (tree_id,))
        self.conn.commit()
        return cursor.rowcount > 0

    def save_node(self, tree_id: str, node_id: str, node_data: Dict[str, Any]) -> None:
        """保存单个节点的数据"""
        cursor = self.cursor
        cursor.execute(
            "INSERT OR REPLACE INTO nodes (node_id, tree_id, node_data) VALUES (?, ?, ?)",
            (node_id, tree_id, json.dumps(node_data, ensure_ascii=False))
        )
        self.conn.commit()

    def load_node(self, tree_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """加载单个节点的数据"""
        cursor = self.cursor
        cursor.execute(
            "SELECT node_data FROM nodes WHERE node_id = ? AND tree_id = ?",
            (node_id, tree_id)
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None

    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """删除节点"""
        cursor = self.cursor
        cursor.execute(
            "DELETE FROM nodes WHERE node_id = ? AND tree_id = ?",
            (node_id, tree_id)
        )
        self.conn.commit()
        return cursor.rowcount > 0

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
        cursor = self.cursor

        # 记录并临时禁用外键约束
        cursor.execute("PRAGMA foreign_keys")
        original_fk_state = cursor.fetchone()[0]
        if original_fk_state == 1:
            cursor.execute("PRAGMA foreign_keys = OFF")

        try:
            # 插入/替换时间点数据
            cursor.execute('''
                INSERT OR REPLACE INTO time_series 
                (tree_id, node_id, dimension, timestamp, value, quality, unit)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                tree_id,
                node_id,
                dimension,
                timestamp,  # 【修复】直接传datetime对象，适配器会自动处理
                json.dumps(value, ensure_ascii=False),
                quality,
                unit
            ))

            # 更新维度统计
            self._update_dimension_stats(tree_id, node_id, dimension, timestamp)

            self.conn.commit()
        finally:
            # 恢复外键约束
            if original_fk_state == 1:
                cursor.execute("PRAGMA foreign_keys = ON")

    def _update_dimension_stats(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            timestamp: datetime
    ):
        """更新维度统计信息"""
        cursor = self.cursor

        # 获取当前统计
        cursor.execute('''
            SELECT min_time, max_time, count FROM dimension_stats
            WHERE tree_id = ? AND node_id = ? AND dimension = ?
        ''', (tree_id, node_id, dimension))

        row = cursor.fetchone()

        if row:
            # 【修复】row[0]已经是datetime对象，直接使用！
            min_time = row[0] if row[0] else None
            max_time = row[1] if row[1] else None
            count = row[2] + 1

            new_min = min(min_time, timestamp) if min_time else timestamp
            new_max = max(max_time, timestamp) if max_time else timestamp

            cursor.execute('''
                UPDATE dimension_stats
                SET min_time = ?, max_time = ?, count = ?, updated_at = CURRENT_TIMESTAMP
                WHERE tree_id = ? AND node_id = ? AND dimension = ?
            ''', (new_min, new_max, count, tree_id, node_id, dimension))
        else:
            # 插入新统计
            cursor.execute('''
                INSERT INTO dimension_stats
                (tree_id, node_id, dimension, min_time, max_time, count)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tree_id, node_id, dimension, timestamp, timestamp, 1))

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
        cursor = self.cursor

        # 构建查询
        sql = '''
            SELECT timestamp, value, quality, unit
            FROM time_series
            WHERE tree_id = ? AND node_id = ? AND dimension = ?
        '''
        params = [tree_id, node_id, dimension]

        if start_time:
            sql += " AND timestamp >= ?"
            params.append(start_time)  # 【修复】直接传datetime对象
        if end_time:
            sql += " AND timestamp <= ?"
            params.append(end_time)  # 【修复】直接传datetime对象

        sql += " ORDER BY timestamp ASC"

        if limit and limit > 0:
            sql += " LIMIT ?"
            params.append(limit)

        cursor.execute(sql, params)
        rows = cursor.fetchall()

        result = []
        for row in rows:
            timestamp = row[0]  # 【修复】已经是datetime对象！
            value = json.loads(row[1])
            metadata = {
                'quality': row[2],
                'unit': row[3]
            }
            result.append((timestamp, value, metadata))

        return result

    def get_latest_time_point(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            before_time: Optional[datetime] = None
    ) -> Optional[Tuple[datetime, Any, Dict]]:
        """获取最新的时间点"""
        cursor = self.cursor

        sql = '''
            SELECT timestamp, value, quality, unit
            FROM time_series
            WHERE tree_id = ? AND node_id = ? AND dimension = ?
        '''
        params = [tree_id, node_id, dimension]

        if before_time:
            sql += " AND timestamp <= ?"
            params.append(before_time)  # 【修复】直接传datetime对象

        sql += " ORDER BY timestamp DESC LIMIT 1"

        cursor.execute(sql, params)
        row = cursor.fetchone()

        if row:
            timestamp = row[0]  # 【修复】已经是datetime对象！
            value = json.loads(row[1])
            metadata = {
                'quality': row[2],
                'unit': row[3]
            }
            return (timestamp, value, metadata)

        return None

    def delete_time_points(
            self,
            tree_id: str,
            node_id: str,
            dimension: str,
            before_time: Optional[datetime] = None
    ) -> int:
        """删除时间点

        Args:
            tree_id: 树ID
            node_id: 节点ID
            dimension: 维度名称
            before_time: 删除此时间之前的数据（不包含此时间点）
        """
        cursor = self.cursor

        sql = '''
            DELETE FROM time_series
            WHERE tree_id = ? AND node_id = ? AND dimension = ?
        '''
        params = [tree_id, node_id, dimension]

        if before_time:
            sql += " AND timestamp < ?"  # 不包含边界
            params.append(before_time)

        cursor.execute(sql, params)
        deleted_count = cursor.rowcount

        # 更新统计信息
        if deleted_count > 0:
            self._refresh_dimension_stats(tree_id, node_id, dimension)

        self.conn.commit()
        return deleted_count

    def _refresh_dimension_stats(self, tree_id: str, node_id: str, dimension: str):
        """刷新维度统计信息"""
        cursor = self.cursor

        # 重新计算统计
        cursor.execute('''
            SELECT 
                MIN(timestamp) as min_time,
                MAX(timestamp) as max_time,
                COUNT(*) as count
            FROM time_series
            WHERE tree_id = ? AND node_id = ? AND dimension = ?
        ''', (tree_id, node_id, dimension))

        row = cursor.fetchone()

        if row and row[2] > 0:
            # 更新统计
            cursor.execute('''
                INSERT OR REPLACE INTO dimension_stats
                (tree_id, node_id, dimension, min_time, max_time, count)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tree_id, node_id, dimension, row[0], row[1], row[2]))
        else:
            # 删除统计
            cursor.execute('''
                DELETE FROM dimension_stats
                WHERE tree_id = ? AND node_id = ? AND dimension = ?
            ''', (tree_id, node_id, dimension))

    def get_dimensions(
        self,
        tree_id: str,
        node_id: Optional[str] = None
    ) -> List[str]:
        """获取所有出现过维度名称"""
        cursor = self.cursor

        if node_id:
            cursor.execute('''
                SELECT DISTINCT dimension
                FROM time_series
                WHERE tree_id = ? AND node_id = ?
                ORDER BY dimension
            ''', (tree_id, node_id))
        else:
            cursor.execute('''
                SELECT DISTINCT dimension
                FROM time_series
                WHERE tree_id = ?
                ORDER BY dimension
            ''', (tree_id,))

        rows = cursor.fetchall()
        return [row[0] for row in rows]

    def get_time_range(
            self,
            tree_id: str,
            node_id: str,
            dimension: str
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """获取某个维度数据的时间范围"""
        cursor = self.cursor

        cursor.execute('''
            SELECT min_time, max_time
            FROM dimension_stats
            WHERE tree_id = ? AND node_id = ? AND dimension = ?
        ''', (tree_id, node_id, dimension))

        row = cursor.fetchone()

        if row and row[0] and row[1]:
            return (row[0], row[1])  # 【修复】已经是datetime对象！

        return None, None

    # ========== 工具方法 ==========

    def clear(self):
        """清空所有数据（用于测试）"""
        cursor = self.cursor
        cursor.execute("PRAGMA foreign_keys = OFF")
        cursor.execute("DELETE FROM time_series")
        cursor.execute("DELETE FROM dimension_stats")
        cursor.execute("DELETE FROM node_data")
        cursor.execute("DELETE FROM nodes")
        cursor.execute("DELETE FROM trees")
        cursor.execute("PRAGMA foreign_keys = ON")
        self.conn.commit()

    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            self._connection = None

    def __del__(self):
        """析构时关闭连接"""
        self.close()