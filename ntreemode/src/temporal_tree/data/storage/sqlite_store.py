"""
SQLite数据库存储实现
数据保存在SQLite数据库中，支持复杂查询
"""
import sqlite3
import threading
import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

from ..serializer import JSONSerializer
from .adapter import DataStoreAdapter
from ...exceptions import DataStoreError, TreeNotFoundError, NodeNotFoundError


class SQLiteStore(DataStoreAdapter):
    """SQLite数据库存储实现"""

    def __init__(self, db_path: str, serializer=None):
        """
        初始化SQLite存储

        Args:
            db_path: 数据库文件路径
            serializer: 序列化器，默认为JSONSerializer
        """
        self.db_path = Path(db_path)
        self.serializer = serializer or JSONSerializer()
        self._lock = threading.RLock()

        # 确保目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 确保文件可写
        try:
            # 尝试创建或打开文件
            self.db_path.touch(exist_ok=True)
        except Exception as e:
            raise DataStoreError(
                f"无法创建数据库文件: {e}",
                operation="init",
                store_type="sqlite"
            )

        # 初始化数据库
        self._init_database()

    @contextmanager
    def _get_connection(self):
        """获取数据库连接（上下文管理器）"""
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=30.0
        )
        conn.row_factory = sqlite3.Row  # 返回字典式行

        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_database(self):
        """初始化数据库表结构"""
        with self._get_connection() as conn:
            # 树表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trees (
                    tree_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    config TEXT,  -- JSON字符串
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 节点表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT NOT NULL,
                    tree_id TEXT NOT NULL,
                    parent_id TEXT,
                    name TEXT NOT NULL,
                    ip_address TEXT,
                    metadata TEXT,  -- JSON字符串
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (tree_id, node_id),
                    FOREIGN KEY (tree_id) REFERENCES trees(tree_id) ON DELETE CASCADE
                )
            """)

            # 节点数据表（时间序列数据）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS node_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tree_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    dimension TEXT NOT NULL,
                    value TEXT NOT NULL,  -- JSON字符串
                    timestamp TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (tree_id, node_id) REFERENCES nodes(tree_id, node_id) ON DELETE CASCADE
                )
            """)

            # 创建索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_tree ON nodes(tree_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_ip ON nodes(tree_id, ip_address)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_node_data_main ON node_data(tree_id, node_id, dimension, timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_node_data_time ON node_data(timestamp)")

    def save_tree(self, tree_data: Dict[str, Any]) -> bool:
        """保存树数据"""
        with self._lock:
            tree_id = tree_data.get('tree_id')
            if not tree_id:
                raise DataStoreError("树数据缺少tree_id", operation="save_tree")

            with self._get_connection() as conn:
                # 序列化配置为JSON字符串
                config_dict = self.serializer.serialize_to_dict(
                    tree_data.get('config', {})
                )
                config_json = json.dumps(config_dict, ensure_ascii=False)

                # 插入或更新树
                conn.execute("""
                    INSERT OR REPLACE INTO trees 
                    (tree_id, name, description, config, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    tree_id,
                    tree_data.get('name', ''),
                    tree_data.get('description', ''),
                    config_json,
                    datetime.now()
                ))

                # 保存根节点（如果存在）
                root_node = tree_data.get('root_node')
                if root_node:
                    self._save_node_internal(tree_id, root_node, conn, skip_tree_check=True)

            return True

    def _save_node_internal(self, tree_id: str, node_data: Dict[str, Any], conn,
                           skip_tree_check: bool = False):
        """内部方法：保存节点数据"""
        if not skip_tree_check:
            # 检查树是否存在
            cursor = conn.execute(
                "SELECT 1 FROM trees WHERE tree_id = ?",
                (tree_id,)
            )
            if not cursor.fetchone():
                raise TreeNotFoundError(f"树不存在: {tree_id}")

        node_id = node_data.get('node_id')
        if not node_id:
            raise DataStoreError("节点数据缺少node_id", operation="save_node")

        # 序列化metadata为JSON字符串
        metadata_dict = self.serializer.serialize_to_dict(
            node_data.get('metadata', {})
        )
        metadata_json = json.dumps(metadata_dict, ensure_ascii=False)

        # 插入或更新节点
        conn.execute("""
            INSERT OR REPLACE INTO nodes 
            (node_id, tree_id, parent_id, name, ip_address, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            node_id,
            tree_id,
            node_data.get('parent_id'),
            node_data.get('name', ''),
            node_data.get('ip_address', ''),
            metadata_json
        ))

    def load_tree(self, tree_id: str) -> Optional[Dict[str, Any]]:
        """加载树数据"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM trees WHERE tree_id = ?",
                    (tree_id,)
                )
                row = cursor.fetchone()

                if not row:
                    raise TreeNotFoundError(f"树不存在: {tree_id}")

                # 转换为字典
                tree_data = dict(row)

                # 解析配置（JSON字符串转字典）
                if tree_data.get('config'):
                    try:
                        config_dict = json.loads(tree_data['config'])
                        tree_data['config'] = self.serializer.deserialize_from_dict(config_dict)
                    except json.JSONDecodeError:
                        tree_data['config'] = {}

                # 删除原始JSON字符串
                if 'config' in tree_data and isinstance(tree_data['config'], str):
                    del tree_data['config']

                # 加载所有节点
                tree_data['nodes'] = self.load_all_nodes(tree_id)

                return tree_data

    def delete_tree(self, tree_id: str) -> bool:
        """删除树（级联删除所有相关数据）"""
        with self._lock:
            with self._get_connection() as conn:
                # 检查树是否存在
                cursor = conn.execute(
                    "SELECT 1 FROM trees WHERE tree_id = ?",
                    (tree_id,)
                )
                if not cursor.fetchone():
                    return False

                # 删除树（级联删除节点和节点数据）
                conn.execute("DELETE FROM trees WHERE tree_id = ?", (tree_id,))

                return True

    def list_trees(self) -> List[Dict[str, Any]]:
        """列出所有树"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT t.*, 
                           COUNT(DISTINCT n.node_id) as node_count,
                           COUNT(DISTINCT nd.dimension) as dimension_count
                    FROM trees t
                    LEFT JOIN nodes n ON t.tree_id = n.tree_id
                    LEFT JOIN node_data nd ON t.tree_id = nd.tree_id
                    GROUP BY t.tree_id
                    ORDER BY t.created_at DESC
                """)

                trees = []
                for row in cursor.fetchall():
                    tree_data = dict(row)

                    # 解析配置
                    if tree_data.get('config'):
                        try:
                            config_dict = json.loads(tree_data['config'])
                            tree_data['config'] = self.serializer.deserialize_from_dict(config_dict)
                        except json.JSONDecodeError:
                            tree_data['config'] = {}

                    trees.append(tree_data)

                return trees

    def save_node(self, tree_id: str, node_data: Dict[str, Any]) -> bool:
        """保存节点数据"""
        with self._lock:
            with self._get_connection() as conn:
                self._save_node_internal(tree_id, node_data, conn, skip_tree_check=False)
                return True

    def load_node(self, tree_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """加载节点数据"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT * FROM nodes 
                    WHERE tree_id = ? AND node_id = ?
                """, (tree_id, node_id))

                row = cursor.fetchone()
                if not row:
                    raise NodeNotFoundError(f"节点不存在: {node_id}")

                node_data = dict(row)

                # 解析metadata
                if node_data.get('metadata'):
                    try:
                        metadata_dict = json.loads(node_data['metadata'])
                        node_data['metadata'] = self.serializer.deserialize_from_dict(metadata_dict)
                    except json.JSONDecodeError:
                        node_data['metadata'] = {}

                # 删除原始JSON字符串
                if 'metadata' in node_data and isinstance(node_data['metadata'], str):
                    del node_data['metadata']

                return node_data

    def load_all_nodes(self, tree_id: str) -> List[Dict[str, Any]]:
        """加载树的所有节点"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM nodes WHERE tree_id = ? ORDER BY created_at",
                    (tree_id,)
                )

                nodes = []
                for row in cursor.fetchall():
                    node_data = dict(row)

                    # 解析metadata
                    if node_data.get('metadata'):
                        try:
                            metadata_dict = json.loads(node_data['metadata'])
                            node_data['metadata'] = self.serializer.deserialize_from_dict(metadata_dict)
                        except json.JSONDecodeError:
                            node_data['metadata'] = {}

                    # 删除原始JSON字符串
                    if 'metadata' in node_data and isinstance(node_data['metadata'], str):
                        del node_data['metadata']

                    nodes.append(node_data)

                return nodes

    def delete_node(self, tree_id: str, node_id: str) -> bool:
        """删除节点（级联删除节点数据）"""
        with self._lock:
            with self._get_connection() as conn:
                # 检查节点是否存在
                cursor = conn.execute("""
                    SELECT 1 FROM nodes 
                    WHERE tree_id = ? AND node_id = ?
                """, (tree_id, node_id))

                if not cursor.fetchone():
                    return False

                # 删除节点（级联删除节点数据）
                conn.execute("""
                    DELETE FROM nodes 
                    WHERE tree_id = ? AND node_id = ?
                """, (tree_id, node_id))

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
            with self._get_connection() as conn:
                # 检查树和节点是否存在
                cursor = conn.execute("""
                    SELECT 1 FROM nodes 
                    WHERE tree_id = ? AND node_id = ?
                """, (tree_id, node_id))

                if not cursor.fetchone():
                    raise NodeNotFoundError(f"节点不存在: {node_id}")

                # 序列化值为JSON字符串
                value_dict = self.serializer.serialize_to_dict(value)
                value_json = json.dumps(value_dict, ensure_ascii=False)

                # 插入数据点
                conn.execute("""
                    INSERT INTO node_data 
                    (tree_id, node_id, dimension, value, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    tree_id,
                    node_id,
                    dimension,
                    value_json,
                    timestamp
                ))

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
            with self._get_connection() as conn:
                result = {}

                if dimension:
                    # 查询特定维度
                    query = """
                        SELECT dimension, value, timestamp 
                        FROM node_data 
                        WHERE tree_id = ? AND node_id = ? AND dimension = ?
                    """
                    params = [tree_id, node_id, dimension]

                    # 添加时间范围
                    if start_time:
                        query += " AND timestamp >= ?"
                        params.append(start_time)
                    if end_time:
                        query += " AND timestamp <= ?"
                        params.append(end_time)

                    query += " ORDER BY timestamp DESC"

                    cursor = conn.execute(query, params)
                    data_points = []

                    for row in cursor.fetchall():
                        point = dict(row)
                        # 反序列化值
                        try:
                            value_dict = json.loads(point['value'])
                            point['value'] = self.serializer.deserialize_from_dict(value_dict)
                        except json.JSONDecodeError:
                            point['value'] = None
                        data_points.append(point)

                    if data_points:
                        result[dimension] = data_points
                else:
                    # 查询所有维度
                    query = """
                        SELECT dimension, value, timestamp 
                        FROM node_data 
                        WHERE tree_id = ? AND node_id = ?
                    """
                    params = [tree_id, node_id]

                    # 添加时间范围
                    if start_time:
                        query += " AND timestamp >= ?"
                        params.append(start_time)
                    if end_time:
                        query += " AND timestamp <= ?"
                        params.append(end_time)

                    query += " ORDER BY dimension, timestamp DESC"

                    cursor = conn.execute(query, params)

                    for row in cursor.fetchall():
                        point = dict(row)
                        dim = point['dimension']

                        # 反序列化值
                        try:
                            value_dict = json.loads(point['value'])
                            point['value'] = self.serializer.deserialize_from_dict(value_dict)
                        except json.JSONDecodeError:
                            point['value'] = None

                        if dim not in result:
                            result[dim] = []
                        result[dim].append(point)

                return result

    def exists_tree(self, tree_id: str) -> bool:
        """检查树是否存在"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM trees WHERE tree_id = ?",
                    (tree_id,)
                )
                return cursor.fetchone() is not None

    def exists_node(self, tree_id: str, node_id: str) -> bool:
        """检查节点是否存在"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT 1 FROM nodes 
                    WHERE tree_id = ? AND node_id = ?
                """, (tree_id, node_id))
                return cursor.fetchone() is not None

    def get_tree_stats(self, tree_id: str) -> Dict[str, Any]:
        """获取树统计信息"""
        with self._lock:
            if not self.exists_tree(tree_id):
                raise TreeNotFoundError(f"树不存在: {tree_id}")

            with self._get_connection() as conn:
                # 节点数
                cursor = conn.execute("""
                    SELECT COUNT(*) as node_count 
                    FROM nodes WHERE tree_id = ?
                """, (tree_id,))
                node_count = cursor.fetchone()[0]

                # 维度数和数据点数
                cursor = conn.execute("""
                    SELECT 
                        COUNT(DISTINCT dimension) as dimension_count,
                        COUNT(*) as data_point_count
                    FROM node_data 
                    WHERE tree_id = ?
                """, (tree_id,))
                dim_row = cursor.fetchone()
                dimension_count = dim_row[0] if dim_row else 0
                data_point_count = dim_row[1] if dim_row else 0

                # 最新更新时间
                cursor = conn.execute("""
                    SELECT MAX(updated_at) as last_updated 
                    FROM trees WHERE tree_id = ?
                """, (tree_id,))
                last_updated = cursor.fetchone()[0]

                return {
                    'tree_id': tree_id,
                    'node_count': node_count,
                    'dimension_count': dimension_count,
                    'data_point_count': data_point_count,
                    'last_updated': last_updated
                }

    def get_node_by_ip(self, tree_id: str, ip_address: str) -> Optional[Dict[str, Any]]:
        """通过IP地址获取节点"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT * FROM nodes 
                    WHERE tree_id = ? AND ip_address = ?
                """, (tree_id, ip_address))

                row = cursor.fetchone()
                if not row:
                    return None

                node_data = dict(row)

                # 解析metadata
                if node_data.get('metadata'):
                    try:
                        metadata_dict = json.loads(node_data['metadata'])
                        node_data['metadata'] = self.serializer.deserialize_from_dict(metadata_dict)
                    except json.JSONDecodeError:
                        node_data['metadata'] = {}

                # 删除原始JSON字符串
                if 'metadata' in node_data and isinstance(node_data['metadata'], str):
                    del node_data['metadata']

                return node_data

    def search_nodes(
        self,
        tree_id: str,
        name_pattern: Optional[str] = None,
        metadata_filter: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """搜索节点"""
        with self._lock:
            with self._get_connection() as conn:
                query = "SELECT * FROM nodes WHERE tree_id = ?"
                params = [tree_id]

                if name_pattern:
                    query += " AND name LIKE ?"
                    params.append(f"%{name_pattern}%")

                # TODO: 实现metadata过滤（需要解析JSON）

                cursor = conn.execute(query, params)

                nodes = []
                for row in cursor.fetchall():
                    node_data = dict(row)

                    # 解析metadata
                    if node_data.get('metadata'):
                        try:
                            metadata_dict = json.loads(node_data['metadata'])
                            node_data['metadata'] = self.serializer.deserialize_from_dict(metadata_dict)
                        except json.JSONDecodeError:
                            node_data['metadata'] = {}

                    # 删除原始JSON字符串
                    if 'metadata' in node_data and isinstance(node_data['metadata'], str):
                        del node_data['metadata']

                    nodes.append(node_data)

                return nodes

    def close(self):
        """关闭数据库连接"""
        # SQLite连接自动管理，无需显式关闭
        pass

    def clear(self):
        """清空所有数据（测试用）"""
        with self._lock:
            with self._get_connection() as conn:
                # 关闭外键约束
                conn.execute("PRAGMA foreign_keys = OFF")

                # 清空表
                conn.execute("DELETE FROM node_data")
                conn.execute("DELETE FROM nodes")
                conn.execute("DELETE FROM trees")

                # 重新打开外键约束
                conn.execute("PRAGMA foreign_keys = ON")

                # 清理空间
                conn.execute("VACUUM")

    def backup(self, backup_path: str):
        """创建数据库备份"""
        with self._lock:
            import shutil
            shutil.copy2(self.db_path, backup_path)

    def optimize(self):
        """优化数据库性能"""
        with self._lock:
            with self._get_connection() as conn:
                conn.execute("PRAGMA optimize")
                conn.execute("VACUUM")

    def __str__(self):
        """字符串表示"""
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM trees")
                tree_count = cursor.fetchone()[0]

                cursor = conn.execute("SELECT COUNT(*) FROM nodes")
                node_count = cursor.fetchone()[0]

                cursor = conn.execute("SELECT COUNT(*) FROM node_data")
                data_count = cursor.fetchone()[0]

                return (f"SQLiteStore(db={self.db_path}, trees={tree_count}, "
                        f"nodes={node_count}, data_points={data_count})")