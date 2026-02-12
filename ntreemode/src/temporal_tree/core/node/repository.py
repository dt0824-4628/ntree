"""
节点仓库 - 管理节点集合和树结构
"""
from typing import Dict, List, Optional, Any
from datetime import datetime

from ...exceptions import TreeError, NodeNotFoundError
from .entity import TreeNode
from .factory import NodeFactory


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

    def to_dict(self, include_data: bool = False) -> Dict[str, Any]:
        """
        将整个树转换为字典

        Args:
            include_data: 是否包含维度数据

        Returns:
            树字典表示
        """
        if not self._root:
            return {"root": None, "nodes": {}}

        return {
            "root": self._root.to_dict(include_children=True, include_data=include_data),
            "node_count": self.get_node_count(),
            "tree_depth": self.get_tree_depth(),
            "created_at": datetime.now().isoformat()
        }