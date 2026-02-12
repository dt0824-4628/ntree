"""
节点工厂 - 创建和管理节点
"""
from typing import Dict, Any, Optional, List
from datetime import datetime

from ...interfaces import IIPProvider
from ...exceptions import NodeError, ValidationError
from .entity import TreeNode


class NodeFactory:
    """节点工厂，负责创建和管理节点"""

    def __init__(self, ip_provider: IIPProvider):
        """
        初始化节点工厂

        Args:
            ip_provider: IP地址提供者
        """
        self._ip_provider = ip_provider
        self._nodes: Dict[str, TreeNode] = {}  # node_id -> TreeNode

    def create_root_node(self, name: str, metadata: Optional[Dict] = None) -> TreeNode:
        """创建根节点"""
        # ✅ 先生成 node_id
        node_id = self._generate_node_id()
        ip = self._ip_provider.allocate_root_ip()

        # 先定义默认值
        storage = None
        tree_id = None

        # 从metadata中提取参数
        if metadata:
            storage = metadata.get('storage')
            tree_id = metadata.get('tree_id_for_storage') or metadata.get('tree_id')

        node = TreeNode(
            node_id=node_id,  # ✅ 使用生成的 node_id
            name=name,
            ip=ip,
            level=0,
            storage=storage,
            tree_id=tree_id
        )

        # 设置节点标签
        node.add_tag("root")
        if metadata:
            for key, value in metadata.items():
                if key not in ['storage', 'tree_id', 'tree_id_for_storage']:
                    node.add_tag(f"{key}:{value}")

        return node

    def create_child_node(
            self,
            parent_node: TreeNode,
            name: str,
            metadata: Optional[Dict] = None
    ) -> TreeNode:
        """创建子节点"""
        # ✅ 先生成 node_id
        node_id = self._generate_node_id()
        child_ip = self._ip_provider.allocate_child_ip(parent_node.ip)

        # 先定义默认值
        storage = None
        tree_id = None

        # 从metadata中提取参数
        if metadata:
            storage = metadata.get('storage')
            tree_id = metadata.get('tree_id_for_storage') or metadata.get('tree_id')

        node = TreeNode(
            node_id=node_id,  # ✅ 使用生成的 node_id
            name=name,
            ip=child_ip,
            level=parent_node.level + 1,
            storage=storage,
            tree_id=tree_id
        )

        # 设置父子关系
        parent_node.add_child(node)

        # 设置节点标签
        if metadata:
            for key, value in metadata.items():
                if key not in ['storage', 'tree_id', 'tree_id_for_storage']:
                    node.add_tag(f"{key}:{value}")

        return node

    def _generate_node_id(self) -> str:
        """生成唯一的节点ID"""
        import uuid
        return str(uuid.uuid4())[:8]

    def create_node_from_dict(self, data: Dict[str, Any]) -> TreeNode:
        """
        从字典创建节点

        Args:
            data: 节点数据字典

        Returns:
            创建的节点
        """
        required_fields = ["name", "ip_address", "level"]
        for field in required_fields:
            if field not in data:
                raise ValidationError(
                    message=f"缺少必需字段: {field}",
                    field=field,
                    reason="required_field_missing"
                )

        # 创建节点
        node = TreeNode(
            name=data["name"],
            ip_address=data["ip_address"],
            level=data["level"],
            node_id=data.get("node_id"),
            metadata=data.get("metadata", {})
        )

        # 添加标签
        for tag in data.get("tags", []):
            node.add_tag(tag)

        # 添加数据
        if "data" in data:
            for dimension, time_data in data["data"].items():
                for time_str, value in time_data.items():
                    timestamp = datetime.fromisoformat(time_str)
                    node.set_data(dimension, value, timestamp)

        # 注册节点
        self._register_node(node)
        return node

    def _register_node(self, node: TreeNode) -> None:
        """注册节点"""
        self._nodes[node.node_id] = node

    def get_node(self, node_id: str) -> Optional[TreeNode]:
        """根据ID获取节点"""
        return self._nodes.get(node_id)

    def get_node_by_ip(self, ip_address: str) -> Optional[TreeNode]:
        """根据IP地址获取节点"""
        for node in self._nodes.values():
            if node.ip_address == ip_address:
                return node
        return None

    def delete_node(self, node_id: str) -> bool:
        """删除节点"""
        if node_id not in self._nodes:
            return False

        node = self._nodes[node_id]

        # 从父节点中移除
        if node.parent:
            node.parent.remove_child(node_id)

        # 递归删除子节点
        for child in node.children[:]:  # 使用副本遍历
            self.delete_node(child.node_id)

        # 从注册表中移除
        del self._nodes[node_id]
        return True

    def get_all_nodes(self) -> List[TreeNode]:
        """获取所有节点"""
        return list(self._nodes.values())

    def get_node_count(self) -> int:
        """获取节点数量"""
        return len(self._nodes)

    def find_nodes_by_name(self, name: str) -> List[TreeNode]:
        """根据名称查找节点"""
        return [node for node in self._nodes.values() if node.name == name]

    def find_nodes_by_tag(self, tag: str) -> List[TreeNode]:
        """根据标签查找节点"""
        return [node for node in self._nodes.values() if node.has_tag(tag)]
