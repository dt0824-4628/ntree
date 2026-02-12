#!/usr/bin/env python3
"""
测试存储模块
"""
import sys
import os
import tempfile
import json
from datetime import datetime, timedelta

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree.data.storage import (
    MemoryStore, JSONStore, SQLiteStore, create_store
)


def create_test_tree(tree_id="test_tree"):
    """创建测试树数据"""
    return {
        "tree_id": tree_id,
        "name": "测试树",
        "description": "存储模块测试",
        "root_node": {
            "node_id": "root",
            "name": "根节点",
            "ip_address": "10.0.0.0",
            "metadata": {"type": "root"}
        }
    }


def create_test_node(node_id="node_1", parent_id="root"):
    """创建测试节点数据"""
    return {
        "node_id": node_id,
        "parent_id": parent_id,
        "name": f"测试节点{node_id}",
        "ip_address": f"10.0.0.{node_id.split('_')[1]}",
        "metadata": {
            "type": "device",
            "location": "柴旦",
            "capacity": 1000
        }
    }


def test_memory_store():
    """测试内存存储"""
    print("=== 测试MemoryStore ===")

    store = MemoryStore()

    # 1. 保存树
    tree_data = create_test_tree()
    assert store.save_tree(tree_data), "保存树失败"
    print("✅ 保存树成功")

    # 2. 加载树
    loaded_tree = store.load_tree("test_tree")
    assert loaded_tree["name"] == "测试树"
    print("✅ 加载树成功")

    # 3. 保存节点
    node_data = create_test_node("node_1")
    assert store.save_node("test_tree", node_data), "保存节点失败"
    print("✅ 保存节点成功")

    # 4. 加载节点
    loaded_node = store.load_node("test_tree", "node_1")
    assert loaded_node["name"] == "测试节点node_1"
    print("✅ 加载节点成功")

    # 5. 保存节点数据
    timestamp = datetime.now()
    assert store.save_node_data(
        "test_tree", "node_1", "gas_volume", 1500.5, timestamp
    ), "保存节点数据失败"
    print("✅ 保存节点数据成功")

    # 6. 加载节点数据
    data = store.load_node_data("test_tree", "node_1", "gas_volume")
    assert len(data.get("gas_volume", [])) == 1
    assert data["gas_volume"][0]["value"] == 1500.5
    print("✅ 加载节点数据成功")

    # 7. 列出所有树
    trees = store.list_trees()
    assert len(trees) == 1
    print("✅ 列出树成功")

    # 8. 统计信息
    stats = store.get_tree_stats("test_tree")
    assert stats["node_count"] >= 1
    print("✅ 获取统计信息成功")

    store.clear()
    print("🎉 MemoryStore测试通过\n")


def test_json_store():
    """测试JSON存储"""
    print("=== 测试JSONStore ===")

    # 使用临时文件
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
        tmp_path = tmp.name

    try:
        store = JSONStore(tmp_path)

        # 1. 保存树
        tree_data = create_test_tree("json_tree")
        assert store.save_tree(tree_data), "保存树失败"
        print("✅ 保存树成功")

        # 2. 重新加载存储（模拟重启）
        store2 = JSONStore(tmp_path)
        loaded_tree = store2.load_tree("json_tree")
        assert loaded_tree["name"] == "测试树"
        print("✅ 持久化加载成功")

        # 3. 验证文件存在
        assert os.path.exists(tmp_path), "JSON文件不存在"
        print("✅ 文件保存成功")

        # 4. 查看文件内容
        with open(tmp_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
            assert "trees" in content
        print("✅ 文件内容验证成功")

    finally:
        # 清理
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    print("🎉 JSONStore测试通过\n")


def test_sqlite_store():
    """测试SQLite存储"""
    print("=== 测试SQLiteStore ===")

    # 使用临时文件
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        tmp_path = tmp.name

    try:
        store = SQLiteStore(tmp_path)

        # 1. 保存树
        tree_data = create_test_tree("sqlite_tree")
        assert store.save_tree(tree_data), "保存树失败"
        print("✅ 保存树成功")

        # 2. 保存多个节点
        for i in range(1, 4):
            node = create_test_node(f"node_{i}")
            assert store.save_node("sqlite_tree", node), f"保存节点{i}失败"
        print("✅ 保存多个节点成功")

        # 3. 保存时间序列数据
        base_time = datetime.now()
        for i in range(5):
            timestamp = base_time - timedelta(hours=i)
            store.save_node_data(
                "sqlite_tree", "node_1", "temperature",
                20 + i, timestamp
            )
        print("✅ 保存时间序列数据成功")

        # 4. 时间范围查询
        end_time = base_time
        start_time = base_time - timedelta(hours=3)
        data = store.load_node_data(
            "sqlite_tree", "node_1", "temperature",
            start_time, end_time
        )
        assert len(data.get("temperature", [])) == 4  # 3小时内的4个点
        print("✅ 时间范围查询成功")

        # 5. 搜索节点
        nodes = store.search_nodes("sqlite_tree", name_pattern="测试节点")
        assert len(nodes) >= 3
        print("✅ 节点搜索成功")

        # 6. 重新加载存储
        store2 = SQLiteStore(tmp_path)
        trees = store2.list_trees()
        assert len(trees) == 1
        print("✅ 持久化验证成功")

    finally:
        # 清理
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    print("🎉 SQLiteStore测试通过\n")


def test_create_store():
    """测试工厂函数"""
    print("=== 测试create_store工厂函数 ===")

    # 1. 创建内存存储
    memory_store = create_store("memory")
    assert isinstance(memory_store, MemoryStore)
    print("✅ 创建MemoryStore成功")

    # 2. 创建JSON存储（手动管理临时文件）
    import tempfile
    import os

    # JSON存储测试
    json_temp = tempfile.NamedTemporaryFile(suffix='.json', delete=False)
    json_path = json_temp.name
    json_temp.close()  # 关闭文件句柄，但保留文件

    try:
        json_store = create_store("json", filepath=json_path)
        assert isinstance(json_store, JSONStore)
        print("✅ 创建JSONStore成功")
    finally:
        # 清理
        if os.path.exists(json_path):
            os.unlink(json_path)

    # 3. 创建SQLite存储（手动管理临时文件）
    sqlite_temp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    sqlite_path = sqlite_temp.name
    sqlite_temp.close()  # 关闭文件句柄，但保留文件

    try:
        sqlite_store = create_store("sqlite", db_path=sqlite_path)
        assert isinstance(sqlite_store, SQLiteStore)
        print("✅ 创建SQLiteStore成功")

        # 简单测试SQLite存储功能
        test_tree = {
            "tree_id": "factory_test",
            "name": "工厂函数测试",
            "description": "测试create_store工厂函数"
        }
        assert sqlite_store.save_tree(test_tree), "SQLite存储测试失败"
        loaded = sqlite_store.load_tree("factory_test")
        assert loaded["name"] == "工厂函数测试"
        print("✅ SQLiteStore功能测试成功")

    finally:
        # 清理
        if os.path.exists(sqlite_path):
            os.unlink(sqlite_path)

    print("🎉 工厂函数测试通过\n")


def performance_comparison():
    """性能比较"""
    print("=== 存储性能比较 ===")

    import time

    # 测试数据
    tree_id = "perf_tree"
    node_count = 100
    data_points_per_node = 10

    stores = []

    # MemoryStore
    with tempfile.NamedTemporaryFile(suffix='.json') as tmp_json:
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp_db:
            stores = [
                ("MemoryStore", MemoryStore()),
                ("JSONStore", JSONStore(tmp_json.name)),
                ("SQLiteStore", SQLiteStore(tmp_db.name))
            ]

            results = {}

            for name, store in stores:
                print(f"\n测试 {name}...")

                # 创建树
                start = time.time()
                store.save_tree(create_test_tree(tree_id))

                # 批量保存节点
                for i in range(node_count):
                    node = create_test_node(f"node_{i}")
                    store.save_node(tree_id, node)

                    # 保存数据点
                    for j in range(data_points_per_node):
                        timestamp = datetime.now() - timedelta(hours=j)
                        store.save_node_data(
                            tree_id, f"node_{i}", "gas_flow",
                            i * 100 + j, timestamp
                        )

                save_time = time.time() - start

                # 查询性能
                start = time.time()
                nodes = store.load_all_nodes(tree_id)
                data = store.load_node_data(tree_id, "node_50", "gas_flow")
                query_time = time.time() - start

                results[name] = {
                    "save_time": save_time,
                    "query_time": query_time,
                    "node_count": len(nodes),
                    "data_points": len(data.get("gas_flow", []))
                }

                print(f"  保存时间: {save_time:.3f}s")
                print(f"  查询时间: {query_time:.3f}s")

    # 输出比较结果
    print("\n📊 性能比较结果:")
    for name, result in results.items():
        print(f"  {name}:")
        print(f"    保存 {result['node_count']}节点 + {result['data_points']}数据点: {result['save_time']:.3f}s")
        print(f"    查询时间: {result['query_time']:.3f}s")

    print("\n✅ 性能比较完成\n")


if __name__ == "__main__":
    print("开始测试存储模块...\n")

    try:
        test_memory_store()
        test_json_store()
        test_sqlite_store()
        test_create_store()

        # 可选性能测试（可能较慢）
        # performance_comparison()

        print("🎉 所有存储测试通过！")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)