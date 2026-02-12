"""
燃气输差分析系统基本使用示例 - 修复版
"""
import sys
import os
from datetime import datetime

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree import TemporalTreeSystem

def main():
    """主函数"""
    print("=" * 60)
    print("燃气输差分析系统 - 基本使用示例")
    print("=" * 60)

    # 1. 创建系统实例
    print("\n1. 初始化系统...")
    system = TemporalTreeSystem({
        "system_name": "柴旦燃气输差分析系统",
        "log_level": "INFO",
        "ip_base": "10.0.0.0",
        "max_tree_depth": 10
    })

    system.initialize()

    # 2. 获取系统信息
    print("\n2. 系统信息:")
    info = system.get_system_info()
    print(f"   系统名称: {info['system_name']}")
    print(f"   系统版本: {info['version']}")
    print(f"   运行时间: {info['uptime']}")

    # 3. 创建分析树
    print("\n3. 创建燃气输差分析树...")
    tree_result = system.create_tree(
        tree_id="chaitan_2024",
        name="柴旦区域2024年燃气分析",
        description="青海省柴旦区域2024年度燃气输差分析"
    )

    print(f"   树创建成功: {tree_result['name']}")
    print(f"   树ID: {tree_result['tree_id']}")
    print(f"   根节点: {tree_result['root_node']['name']}")

    # 4. 添加节点（模拟实际结构）
    print("\n4. 构建树结构...")

    # 从树中获取实际的根节点
    tree = system.get_tree("chaitan_2024")
    root_node = tree.root
    root_id = root_node.node_id

    # 添加结算层级
    settlement = system.add_node(
        tree_id="chaitan_2024",
        parent_node_id=root_id,
        name="上游结算",
        metadata={"type": "settlement", "description": "上游气量结算点"}
    )
    print(f"   添加节点: {settlement['name']} (IP: {settlement['ip_address']})")

    # 添加场站设备
    station = system.add_node(
        tree_id="chaitan_2024",
        parent_node_id=settlement["node_id"],
        name="场站设备",
        metadata={"type": "station", "description": "燃气场站设备层"}
    )
    print(f"   添加节点: {station['name']} (IP: {station['ip_address']})")

    # 添加具体设备
    devices = [
        ("S001+L+上游主路", "main_line"),
        ("S002+L+上游副路", "secondary_line"),
        ("S003+L+备用管路", "backup_line")
    ]

    for device_name, line_type in devices:
        device = system.add_node(
            tree_id="chaitan_2024",
            parent_node_id=station["node_id"],
            name=device_name,
            metadata={
                "type": "device",
                "device_id": device_name[:4],
                "line_type": line_type,
                "description": f"{line_type}设备"
            }
        )
        print(f"   添加设备: {device['name']} (IP: {device['ip_address']})")

    # 5. 设置燃气数据
    print("\n5. 设置燃气数据（2024年1月）...")

    # 获取设备节点
    device_nodes = []
    for node in tree.get_all_nodes():
        metadata = getattr(node, '_metadata', {})
        if isinstance(metadata, dict) and metadata.get('type') == 'device':
            device_nodes.append(node)

    if not device_nodes:
        print("   ⚠ 未找到设备节点，跳过数据设置")
    else:
        # 为每个设备设置数据
        time_jan = datetime(2024, 1, 31, 23, 59, 59)
        gas_data = [
            (15000.0, 14250.0),  # 设备1：标准15000，表计14250
            (8000.0, 7600.0),    # 设备2：标准8000，表计7600
            (5000.0, 4850.0)     # 设备3：标准5000，表计4850
        ]

        # 确保设备数量和数据匹配
        device_count = min(len(device_nodes), len(gas_data))

        for i in range(device_count):
            device = device_nodes[i]
            standard, meter = gas_data[i]

            # 设置标准气量
            system.set_node_data(
                tree_id="chaitan_2024",
                node_id=device.node_id,
                dimension="standard_gas",
                value=standard,
                timestamp=time_jan
            )

            # 设置表计气量
            system.set_node_data(
                tree_id="chaitan_2024",
                node_id=device.node_id,
                dimension="meter_gas",
                value=meter,
                timestamp=time_jan
            )

            # 计算输差率
            loss_rate = system.calculate_node_dimension(
                tree_id="chaitan_2024",
                node_id=device.node_id,
                dimension="loss_rate",
                timestamp=time_jan
            )

            print(f"   设备{i+1} {device.name}:")
            print(f"     标准气量: {standard:,.2f} m³")
            print(f"     表计气量: {meter:,.2f} m³")
            print(f"     输差率: {loss_rate*100:.2f}%")

    # 6. 输差分析
    print("\n6. 进行输差分析...")
    try:
        analysis = system.analyze_loss_rate("chaitan_2024", threshold=0.05)

        print(f"   总体统计:")
        print(f"     总标准气量: {analysis['overall']['total_standard_gas']:,.2f} m³")
        print(f"     总表计气量: {analysis['overall']['total_meter_gas']:,.2f} m³")
        print(f"     总体输差率: {analysis['overall']['loss_rate_percent']}")
        print(f"     分析节点数: {analysis['overall']['node_count']}")

        if analysis['high_loss_nodes']:
            print(f"\n   高输差节点 ({analysis['high_loss_count']}个):")
            for node in analysis['high_loss_nodes']:
                print(f"     - {node['name']}: {node['loss_rate_percent']} "
                      f"(标准: {node['standard_gas']:,.0f} m³, "
                      f"表计: {node['meter_gas']:,.0f} m³)")
        else:
            print(f"\n   所有节点输差率均在正常范围内 (<5%)")
    except Exception as e:
        print(f"   ⚠ 输差分析失败: {e}")

    # 7. 导出数据
    print("\n7. 导出分析数据...")
    try:
        tree_data = system.export_tree("chaitan_2024", include_data=True, format="dict")

        print(f"   树节点总数: {tree_data['node_count']}")
        print(f"   树最大深度: {tree_data['tree_depth']}")
        print(f"   导出时间: {tree_data['created_at']}")
    except Exception as e:
        print(f"   ⚠ 导出数据失败: {e}")

    # 8. 系统状态
    print("\n8. 系统状态检查...")
    health = system.health_check()
    print(f"   系统状态: {health['status']}")
    print(f"   管理树数量: {health['trees']['count']}")

    # 9. 列出所有树
    print("\n9. 系统管理的所有树:")
    trees = system.list_trees()
    for tree_info in trees:
        print(f"   - {tree_info['name']} (ID: {tree_info['tree_id']})")
        print(f"     节点数: {tree_info['node_count']}, "
              f"深度: {tree_info['tree_depth']}, "
              f"创建时间: {tree_info['created_at'][:19]}")

    print("\n" + "=" * 60)
    print("示例运行完成！")
    print("=" * 60)

    return True

if __name__ == "__main__":
    try:
        main()
        print("\n🎉 燃气输差分析系统演示成功！")
    except Exception as e:
        print(f"\n❌ 示例运行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)