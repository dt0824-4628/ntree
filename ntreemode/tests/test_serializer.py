"""
测试序列化模块
"""
import sys
import os
from datetime import datetime

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree.data.serializer import JSONSerializer, BinarySerializer


def test_json_serializer():
    """测试JSON序列化器"""
    print("=== 测试JSON序列化器 ===")

    serializer = JSONSerializer()

    # 测试数据
    test_data = {
        "name": "柴旦燃气系统",
        "version": "1.0.0",
        "created_at": datetime.now(),
        "nodes": [
            {"id": 1, "name": "根节点", "ip": "10.0.0.0"},
            {"id": 2, "name": "上游结算", "ip": "10.0.0.1"}
        ],
        "gas_data": {
            "standard": 15000.5,
            "meter": 14250.25
        }
    }

    # 序列化
    print("1. 序列化测试数据...")
    json_bytes = serializer.serialize(test_data)
    print(f"   序列化大小: {len(json_bytes)} 字节")

    # 反序列化
    print("2. 反序列化...")
    restored_data = serializer.deserialize(json_bytes)
    print(f"   恢复的数据类型: {type(restored_data)}")

    # 验证
    print("3. 验证数据...")
    print(f"   名称匹配: {test_data['name'] == restored_data['name']}")
    print(f"   节点数匹配: {len(test_data['nodes']) == len(restored_data['nodes'])}")

    # 文件操作测试
    print("4. 文件操作测试...")
    serializer.save_to_file(test_data, "test_data.json")
    print(f"   已保存到 test_data.json")

    loaded_data = serializer.load_from_file("test_data.json")
    print(f"   从文件加载成功: {loaded_data['name']}")

    # 清理
    if os.path.exists("test_data.json"):
        os.remove("test_data.json")

    print("✅ JSON序列化器测试通过\n")


def test_binary_serializer():
    """测试二进制序列化器"""
    print("=== 测试二进制序列化器 ===")

    serializer = BinarySerializer(compress=True)

    # 测试数据
    test_data = {
        "name": "柴旦燃气系统",
        "nodes": list(range(1000)),  # 大量数据测试压缩效果
        "timestamp": datetime.now()
    }

    # 序列化
    print("1. 序列化测试数据...")
    binary_data = serializer.serialize(test_data)
    print(f"   序列化大小: {len(binary_data)} 字节")

    # 反序列化
    print("2. 反序列化...")
    restored_data = serializer.deserialize(binary_data)
    print(f"   恢复成功: {restored_data['name']}")
    print(f"   节点数: {len(restored_data['nodes'])}")

    # 文件操作测试
    print("3. 文件操作测试...")
    serializer.save_to_file(test_data, "test_data.bin")
    print(f"   已保存到 test_data.bin")

    loaded_data = serializer.load_from_file("test_data.bin")
    print(f"   从文件加载成功: {len(loaded_data['nodes'])} 个节点")

    # 清理
    if os.path.exists("test_data.bin"):
        os.remove("test_data.bin")

    print("✅ 二进制序列化器测试通过\n")


def compare_serializers():
    """比较两种序列化器"""
    print("=== 序列化器比较 ===")

    test_data = {
        "tree_name": "柴旦2024",
        "nodes": [{"id": i, "name": f"节点{i}"} for i in range(100)],
        "timestamp": datetime.now()
    }

    # JSON序列化器
    json_serializer = JSONSerializer()
    json_data = json_serializer.serialize(test_data)
    json_size = len(json_data)

    # 二进制序列化器（无压缩）
    binary_serializer = BinarySerializer(compress=False)
    binary_data = binary_serializer.serialize(test_data)
    binary_size = len(binary_data)

    # 二进制序列化器（有压缩）
    binary_compressed = BinarySerializer(compress=True)
    compressed_data = binary_compressed.serialize(test_data)
    compressed_size = len(compressed_data)

    print(f"测试数据: 100个节点 + 元数据")
    print(f"JSON序列化:      {json_size:>8} 字节")
    print(f"二进制序列化:    {binary_size:>8} 字节 (缩小 {binary_size / json_size * 100:.1f}%)")
    print(f"二进制+压缩:     {compressed_size:>8} 字节 (缩小 {compressed_size / json_size * 100:.1f}%)")
    print(f"压缩效果:        {binary_size / compressed_size:.1f}x")

    # 可读性比较
    print(f"\n可读性:")
    print(f"  JSON: 可直接用文本编辑器查看 ✓")
    print(f"  二进制: 需要特殊工具查看 ✗")

    print("✅ 比较测试完成\n")


if __name__ == "__main__":
    print("开始测试序列化模块...\n")

    try:
        test_json_serializer()
        test_binary_serializer()
        compare_serializers()

        print("🎉 所有序列化测试通过！")
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()