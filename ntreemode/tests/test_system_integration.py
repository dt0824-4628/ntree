# -*- coding: utf-8 -*-
"""
系统集成测试
测试IP模块、节点模块、维度模块的集成
"""
import sys
import os
from datetime import datetime

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def test_system_integration():
    """测试整个系统集成"""
    print("测试系统集成...")

    # 导入所有模块
    from temporal_tree.core.ip import IncrementalIPProvider
    from temporal_tree.core.node import NodeFactory, NodeRepository
    from temporal_tree.data.dimensions import DimensionRegistry
    from temporal_tree.exceptions import NodeError, ValidationError

    # 1. 初始化所有组件
    print("1. 初始化组件...")
    ip_provider = IncrementalIPProvider(base_ip="10.0.0.0")
    node_factory = NodeFactory(ip_provider)
    dimension_registry = DimensionRegistry()

    # 2. 创建燃气输差分析树
    print("2. 创建燃气输差分析树...")

    # 创建根节点（区域）
    root = node_factory.create_root_node("柴旦区域", {
        "type": "region",
        "description": "青海省柴旦燃气供应区域"
    })

    # 创建子节点（结算层级）
    settlement = node_factory.create_child_node(root, "上游结算", {
        "type": "settlement",
        "description": "上游气量结算点"
    })

    # 创建孙节点（场站设备）
    station = node_factory.create_child_node(settlement, "场站设备", {
        "type": "station",
        "description": "燃气场站设备层"
    })

    # 创建具体设备节点
    device1 = node_factory.create_child_node(station, "S001+L+上游主路", {
        "type": "device",
        "device_id": "S001",
        "description": "上游主路设备"
    })

    device2 = node_factory.create_child_node(station, "S002+L+上游副路", {
        "type": "device",
        "device_id": "S002",
        "description": "上游副路设备"
    })

    print(f"  创建了 {node_factory.get_node_count()} 个节点")

    # 3. 设置燃气数据（模拟2024年1月数据）
    print("3. 设置燃气数据...")

    # 设备1数据
    time_jan = datetime(2024, 1, 31, 23, 59, 59)
    device1.set_data("standard_gas", 15000.0, time_jan)  # 标准气量
    device1.set_data("meter_gas", 14250.0, time_jan)  # 表计气量
    device1.add_tag("main_line")
    device1.add_tag("critical")

    # 设备2数据
    device2.set_data("standard_gas", 8000.0, time_jan)
    device2.set_data("meter_gas", 7600.0, time_jan)
    device2.add_tag("secondary_line")

    print(f"  设备1标准气量: {device1.get_data('standard_gas'):,.2f} m³")
    print(f"  设备1表计气量: {device1.get_data('meter_gas'):,.2f} m³")

    # 4. 计算输差率
    print("4. 计算输差率...")

    # 为节点添加输差率计算器
    def calculate_loss_rate(node, timestamp=None):
        return dimension_registry.calculate_dimension("loss_rate", node, timestamp)

    device1.add_dimension_calculator("loss_rate", calculate_loss_rate)
    device2.add_dimension_calculator("loss_rate", calculate_loss_rate)

    # 获取输差率
    loss_rate1 = device1.get_data("loss_rate", time_jan)
    loss_rate2 = device2.get_data("loss_rate", time_jan)

    print(f"  设备1输差率: {loss_rate1 * 100:.2f}%")
    print(f"  设备2输差率: {loss_rate2 * 100:.2f}%")

    # 验证计算正确性
    expected1 = (15000.0 - 14250.0) / 15000.0
    expected2 = (8000.0 - 7600.0) / 8000.0
    assert abs(loss_rate1 - expected1) < 0.001
    assert abs(loss_rate2 - expected2) < 0.001
    print("  ✓ 输差率计算正确")

    # 5. 创建节点仓库管理整棵树
    print("5. 创建节点仓库...")
    repository = NodeRepository(root)

    # 验证树结构
    assert repository.get_node_count() == 5
    assert repository.get_tree_depth() == 3
    print(f"  树节点数: {repository.get_node_count()}")
    print(f"  树深度: {repository.get_tree_depth()}")

    # 6. 测试查询功能
    print("6. 测试查询功能...")

    # 查找所有设备节点 - 使用自定义查询
    devices = []
    for node in repository.get_all_nodes():
        metadata = getattr(node, '_metadata', {})
        if isinstance(metadata, dict) and metadata.get('type') == 'device':
            devices.append(node)

    print(f"  找到 {len(devices)} 个设备节点: {[d.name for d in devices]}")
    # 检查是否找到预期的设备
    device_names = [d.name for d in devices]
    assert "S001+L+上游主路" in device_names
    assert "S002+L+上游副路" in device_names
    print("  ✓ 设备查询测试通过")

    # 查找关键设备
    critical_devices = [node for node in repository.get_all_nodes()
                        if hasattr(node, 'has_tag') and node.has_tag("critical")]
    print(f"  找到 {len(critical_devices)} 个关键设备")

    # 7. 测试数据验证
    print("7. 测试数据验证...")

    # 验证维度数据
    assert dimension_registry.validate_dimension_data("standard_gas", 1000.0) is True
    assert dimension_registry.validate_dimension_data("standard_gas", -100.0) is False
    print("  ✓ 维度数据验证正常")

    # 验证节点数据
    assert device1.validate() is True
    print("  ✓ 节点验证正常")

    # 8. 测试数据导出
    print("8. 测试数据导出...")

    # 导出整棵树
    tree_dict = repository.to_dict(include_data=True)
    assert tree_dict["node_count"] == 5
    assert tree_dict["tree_depth"] == 3

    # 导出节点详情
    device1_dict = device1.to_dict(include_data=True)
    assert device1_dict["name"] == "S001+L+上游主路"
    assert "standard_gas" in device1_dict.get("data", {})

    print("  ✓ 数据导出正常")

    # 9. 测试IP地址系统
    print("9. 测试IP地址系统...")

    assert device1.ip_address == "10.0.0.0.0.0.0"
    assert device2.ip_address == "10.0.0.0.0.0.1"

    # 验证IP层级
    assert ip_provider.get_ip_level(device1.ip_address) == 3
    assert ip_provider.get_ip_level(root.ip_address) == 0

    print(f"  设备1 IP: {device1.ip_address} (层级: {device1.level})")
    print(f"  根节点 IP: {root.ip_address} (层级: {root.level})")

    # 10. 模拟时间序列数据（多个月份）
    print("10. 模拟时间序列数据...")

    months = [
        datetime(2024, 1, 31),
        datetime(2024, 2, 29),
        datetime(2024, 3, 31),
    ]

    # 为设备1添加多个月份数据
    monthly_standard = [15000.0, 15500.0, 16000.0]
    monthly_meter = [14250.0, 14645.0, 15040.0]

    for i, (month, std, meter) in enumerate(zip(months, monthly_standard, monthly_meter)):
        device1.set_data("standard_gas", std, month)
        device1.set_data("meter_gas", meter, month)

        # 计算每月输差率
        loss_rate = calculate_loss_rate(device1, month)
        print(f"  2024年{i + 1}月输差率: {loss_rate * 100:.2f}%")

    # 获取历史数据
    jan_data = device1.get_data("standard_gas", months[0])
    mar_data = device1.get_data("standard_gas", months[2])
    assert jan_data == 15000.0
    assert mar_data == 16000.0

    print("  ✓ 时间序列数据处理正常")

    print("\n" + "=" * 60)
    print("系统集成测试完成！")
    print("=" * 60)
    print(f"组件集成: ✓ IP模块、节点模块、维度模块")
    print(f"业务功能: ✓ 树结构、燃气数据、输差计算")
    print(f"数据管理: ✓ 时间序列、数据验证、查询导出")
    print("=" * 60)

    return True


def test_real_world_scenario():
    """测试真实业务场景"""
    print("\n测试真实业务场景...")

    from temporal_tree.core.ip import IncrementalIPProvider
    from temporal_tree.core.node import NodeFactory, NodeRepository
    from temporal_tree.data.dimensions import DimensionRegistry
    from datetime import datetime
    import random

    # 初始化
    ip_provider = IncrementalIPProvider(base_ip="192.168.1.0")
    factory = NodeFactory(ip_provider)
    registry = DimensionRegistry()

    # 场景：区域燃气输差分析
    print("构建区域燃气输差分析树...")

    # 1. 省级节点
    province = factory.create_root_node("青海省", {
        "type": "province",
        "area": "西北地区"
    })
    repo = NodeRepository(province)  # 直接传入根节点创建仓库

    # 2. 市级节点
    cities = ["西宁市", "海东市", "海西州"]
    city_nodes = []

    for city_name in cities:
        city = factory.create_child_node(province, city_name, {
            "type": "city",
            "administration_level": "市级"
        })
        city_nodes.append(city)

    print(f"  创建了 {len(city_nodes)} 个城市节点")

    # 3. 区县级节点（以西宁市为例）
    xining = city_nodes[0]
    districts = ["城东区", "城中区", "城西区", "城北区"]

    for district_name in districts:
        district = factory.create_child_node(xining, district_name, {
            "type": "district",
            "administration_level": "区县级"
        })

    print(f"  创建了 {len(districts)} 个区县节点")

    # 4. 场站节点（以城东区为例）
    # 需要先找到城东区节点
    chengdong = None
    for node in repo.get_all_nodes():
        if node.name == "城东区":
            chengdong = node
            break

    if chengdong:
        stations = ["城东门站", "韵家口调压站", "乐家湾储配站"]

        for station_name in stations:
            station = factory.create_child_node(chengdong, station_name, {
                "type": "station",
                "function": "输配气"
            })

        print(f"  创建了 {len(stations)} 个场站节点")

    # 5. 设置燃气数据
    print("设置燃气数据...")

    # 为每个场站设置数据
    stations_nodes = []
    for node in repo.get_all_nodes():
        metadata = getattr(node, '_metadata', {})
        if isinstance(metadata, dict) and metadata.get('type') == 'station':
            stations_nodes.append(node)

    for station in stations_nodes:
        # 随机生成气量数据（模拟实际数据）
        standard_gas = random.uniform(50000, 200000)
        meter_gas = standard_gas * random.uniform(0.93, 0.98)  # 输差率在2%-7%之间

        station.set_data("standard_gas", round(standard_gas, 2))
        station.set_data("meter_gas", round(meter_gas, 2))

        # 添加计算器
        def make_calculator(reg, stat=station):
            def calculator(node, timestamp=None):
                return reg.calculate_dimension("loss_rate", node, timestamp)

            return calculator

        station.add_dimension_calculator("loss_rate", make_calculator(registry))

    # 6. 分析统计
    print("进行输差分析...")

    total_standard = 0
    total_meter = 0
    station_count = len(stations_nodes)
    high_loss_stations = []

    for station in stations_nodes:
        standard = station.get_data("standard_gas") or 0
        meter = station.get_data("meter_gas") or 0
        loss_rate = station.get_data("loss_rate") or 0

        total_standard += standard
        total_meter += meter

        if loss_rate > 0.05:  # 输差率大于5%
            high_loss_stations.append({
                "name": station.name,
                "loss_rate": f"{loss_rate * 100:.1f}%",
                "ip": station.ip_address
            })

    # 计算区域总输差率
    if total_standard > 0:
        region_loss_rate = (total_standard - total_meter) / total_standard
        print(f"区域总标准气量: {total_standard:,.2f} m³")
        print(f"区域总表计气量: {total_meter:,.2f} m³")
        print(f"区域总输差率: {region_loss_rate * 100:.2f}%")

    print(f"场站总数: {station_count}")
    print(f"高输差场站数: {len(high_loss_stations)}")

    if high_loss_stations:
        print("高输差场站列表:")
        for station in high_loss_stations:
            print(f"  - {station['name']}: {station['loss_rate']} (IP: {station['ip']})")

    # 7. 导出分析报告
    print("\n生成分析报告...")

    report = {
        "analysis_date": datetime.now().isoformat(),
        "region": "青海省西宁市城东区",
        "station_count": station_count,
        "total_standard_gas": total_standard,
        "total_meter_gas": total_meter,
        "region_loss_rate": region_loss_rate if total_standard > 0 else 0,
        "high_loss_stations": high_loss_stations,
        "tree_info": {
            "total_nodes": repo.get_node_count(),
            "tree_depth": repo.get_tree_depth(),
            "ip_system": "增量编码系统"
        }
    }

    print(f"分析报告生成完成:")
    print(f"  分析区域: {report['region']}")
    print(f"  场站数量: {report['station_count']}")
    print(f"  总输差率: {report['region_loss_rate'] * 100:.2f}%")
    print(f"  高输差场站: {len(report['high_loss_stations'])}个")

    return True

def run_all_tests():
    """运行所有集成测试"""
    print("=" * 60)
    print("系统集成测试")
    print("=" * 60)

    tests = [
        ("系统集成测试", test_system_integration),
        ("真实业务场景测试", test_real_world_scenario),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ 测试失败: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"集成测试结果: {passed}/{total} 通过")
    print("=" * 60)

    if passed == total:
        print("\n🎉 恭喜！系统集成测试全部通过！")
        print("系统包含以下功能模块:")
        print("  1. ✅ IP增量编码系统")
        print("  2. ✅ 树节点管理系统")
        print("  3. ✅ 燃气数据维度系统")
        print("  4. ✅ 输差率计算分析")
        print("  5. ✅ 时间序列数据管理")
        print("  6. ✅ 业务场景模拟")

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)