"""
节点仓库模块
管理树节点的存储、查询和遍历
"""

from typing import Optional, Dict, Any, List, Callable, Iterator
from collections import deque
from datetime import datetime  # ✅ 添加 datetime 导入

from .entity import TreeNode
from ...exceptions import NodeNotFoundError, TreeNotFoundError  # ✅ 添加 TreeNotFoundError
from ...data.storage.adapter import DataStoreAdapter
from ..time.timeline import Timeline  # ✅ 添加 Timeline 导入！


class NodeRepository:
    """节点仓库，管理节点集合和树结构"""

    def __init__(self, root_node: Optional[TreeNode] = None):
        """
        初始化节点仓库

        Args:
            root_node: 根节点，如果为None则创建一个空的仓库
        """
        self._root = root_node
        self._nodes: Dict[str, TreeNode] = {}

        if root_node:
            self._register_node_and_descendants(root_node)

    def _register_node_and_descendants(self, node: TreeNode) -> None:
        """注册节点及其所有后代"""
        self._nodes[node.node_id] = node

        for child in node.children:
            self._register_node_and_descendants(child)

    @property
    def root(self) -> Optional[TreeNode]:
        """获取根节点"""
        return self._root

    def set_root(self, root_node: TreeNode) -> None:
        """设置根节点"""
        if self._root is not None:
            raise TreeError("根节点已设置")

        self._root = root_node
        self._register_node_and_descendants(root_node)

    def get_node(self, node_id: str) -> Optional[TreeNode]:
        """根据ID获取节点"""
        return self._nodes.get(node_id)

    def get_node_by_ip(self, ip_address: str) -> Optional[TreeNode]:
        """根据IP地址获取节点"""
        for node in self._nodes.values():
            if node.ip_address == ip_address:
                return node
        return None

    def add_node(self, node: TreeNode, parent_id: Optional[str] = None) -> TreeNode:
        if node.node_id in self._nodes:
            return node  # 已存在

        # 注册节点
        self._nodes[node.node_id] = node

        # 建立父子关系（如果指定）
        if parent_id:
            parent = self.get_node(parent_id)
            if parent:
                parent.add_child(node)

        return node

    def remove_node(self, node_id: str) -> bool:
        """
        移除节点

        Args:
            node_id: 节点ID

        Returns:
            是否成功移除
        """
        if node_id not in self._nodes:
            return False

        node = self._nodes[node_id]

        # 如果是根节点
        if node == self._root:
            self._root = None

        # 从父节点中移除
        if node.parent:
            node.parent.remove_child(node_id)

        # 递归移除所有后代节点
        descendants = node.get_descendants()
        for descendant in descendants:
            if descendant.node_id in self._nodes:
                del self._nodes[descendant.node_id]

        # 从仓库中移除
        del self._nodes[node_id]
        return True

    def get_all_nodes(self) -> List[TreeNode]:
        """获取所有节点"""
        return list(self._nodes.values())

    def get_node_count(self) -> int:
        """获取节点数量"""
        return len(self._nodes)

    def get_tree_depth(self) -> int:
        """获取树的最大深度"""
        if not self._root:
            return 0

        max_depth = 0

        def calculate_depth(node: TreeNode, current_depth: int):
            nonlocal max_depth
            max_depth = max(max_depth, current_depth)

            for child in node.children:
                calculate_depth(child, current_depth + 1)

        calculate_depth(self._root, 0)
        return max_depth

    def find_nodes(self, **criteria) -> List[TreeNode]:
        """
        根据条件查找节点

        Args:
            **criteria: 查找条件，如 name="柴旦", level=0

        Returns:
            匹配的节点列表
        """
        results = []

        for node in self._nodes.values():
            match = True

            for key, value in criteria.items():
                if not hasattr(node, key):
                    match = False
                    break

                node_value = getattr(node, key)
                if callable(node_value):
                    node_value = node_value()

                if node_value != value:
                    match = False
                    break

            if match:
                results.append(node)

        return results

    def traverse(self, order: str = "preorder") -> List[TreeNode]:
        """
        遍历树

        Args:
            order: 遍历顺序，可选 "preorder"（前序）, "inorder"（中序）, "postorder"（后序）

        Returns:
            节点列表
        """
        if not self._root:
            return []

        result = []

        def preorder(node: TreeNode):
            result.append(node)
            for child in node.children:
                preorder(child)

        def postorder(node: TreeNode):
            for child in node.children:
                postorder(child)
            result.append(node)

        if order == "preorder":
            preorder(self._root)
        elif order == "postorder":
            postorder(self._root)
        else:
            raise ValueError(f"不支持的遍历顺序: {order}")

        return result

    def to_dict(self, include_children: bool = True, include_data: bool = True) -> Dict[str, Any]:
        """
        序列化节点

        Args:
            include_children: 是否包含子节点ID列表
            include_data: 是否包含维度数据

        Returns:
            可JSON序列化的字典
        """
        result = {
            'node_id': self.node_id,
            'name': self.name,
            'ip': str(self.ip),
            'level': self.level,
            'tags': list(self._tags),
            'created_at': self.created_at.isoformat(),
            'deleted_at': self.deleted_at.isoformat() if self.deleted_at else None,
            'is_active': self.is_active,
            'parent_id': self.parent.node_id if self.parent else None,  # ✅ 添加 parent_id！
        }

        if include_children:
            result['children'] = [child.node_id for child in self.children]

        if include_data:
            result['timelines'] = {
                dim: tl.to_dict()
                for dim, tl in self._timelines.items()
            }

        return result

    # ===== 存储 =====
    def save_to_storage(self, storage: DataStoreAdapter, tree_id: str):
        """将内存中的整棵树保存到存储"""
        print(f"\n💾 保存树到存储: {tree_id}")

        # 1. 准备完整的树数据
        tree_data = {
            'tree_id': tree_id,
            'root_node': self.root.to_dict(),
            'nodes': {},
            'metadata': {
                'node_count': len(self.get_all_nodes()),
                'tree_depth': self.get_tree_depth(),
                'saved_at': datetime.now().isoformat()
            }
        }

        # 2. 保存所有节点数据到 tree_data
        all_nodes = self.get_all_nodes()
        print(f"   共 {len(all_nodes)} 个节点")

        for node in all_nodes:
            node_dict = node.to_dict()
            # ✅ 确保 parent_id 被正确保存
            node_dict['parent_id'] = node.parent.node_id if node.parent else None
            tree_data['nodes'][node.node_id] = node_dict
            print(
                f"   - 添加节点: {node.name} ({node.node_id[:8]}), 父节点: {node_dict['parent_id'][:8] if node_dict['parent_id'] else 'None'}")

        # 3. 保存到存储
        storage.save_tree(tree_id, tree_data)
        print(f"   ✅ 树结构保存成功")

        # 4. 单独保存每个节点（兼容老接口）
        for node in all_nodes:
            storage.save_node(tree_id, node.node_id, node.to_dict())

        # 5. 保存所有时间线数据
        timeline_count = 0
        for node in all_nodes:
            for dim, tl in node._timelines.items():
                for ts, point in tl._time_points.items():
                    storage.save_time_point(
                        tree_id=tree_id,
                        node_id=node.node_id,
                        dimension=dim,
                        timestamp=ts,
                        value=point.value,
                        quality=point.metadata.get('quality', 1),
                        unit=point.metadata.get('unit')
                    )
                    timeline_count += 1

        print(f"   ✅ {timeline_count} 条时间线数据保存成功")

    @classmethod
    def load_from_storage(cls, storage: DataStoreAdapter, tree_id: str):
        """从存储加载整棵树到内存"""
        print(f"\n🔍 开始加载树: {tree_id}")

        # 1. 加载树数据
        tree_data = storage.load_tree(tree_id)
        if not tree_data:
            raise TreeNotFoundError(tree_id)

        # 2. 获取所有节点数据
        nodes_dict = tree_data.get('nodes', {})
        if not nodes_dict:
            raise ValueError("树数据中没有节点信息")

        # 3. 第一遍：创建所有节点对象
        temp_nodes = {}
        for node_id, node_data in nodes_dict.items():
            node = TreeNode.from_dict(node_data)
            temp_nodes[node_id] = node

        # 4. 找出根节点（parent_id 为 None 的节点）
        root = None
        for node_id, node in temp_nodes.items():
            node_data = nodes_dict[node_id]
            if node_data.get('parent_id') is None:
                root = node
                break

        if not root:
            raise ValueError("找不到根节点")

        # 5. 创建仓库
        repo = cls(root)
        repo._nodes = {}  # 清空默认的 _nodes
        repo._nodes[root.node_id] = root

        # 6. 第二遍：建立父子关系
        for node_id, node in temp_nodes.items():
            if node_id == root.node_id:
                continue

            node_data = nodes_dict[node_id]
            parent_id = node_data.get('parent_id')

            if parent_id and parent_id in temp_nodes:
                parent = temp_nodes[parent_id]
                parent.add_child(node)
                repo._nodes[node_id] = node

        # 7. 验证节点数量
        print(f"   ✅ 共加载 {len(repo._nodes)} 个节点")

        # 8. 加载时间线数据...

        return repo
