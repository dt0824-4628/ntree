from datetime import datetime, timedelta
import os
import sys
import random

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from temporal_tree.core.ip.provider import IncrementalIPProvider
from temporal_tree.core.node.factory import NodeFactory
from temporal_tree.core.node.repository import NodeRepository
from temporal_tree.data.storage.sqlite_store import SQLiteStore
from temporal_tree.data.storage.json_store import JSONStore
from temporal_tree.data.storage.memory_store import MemoryStore
from temporal_tree.data.dimensions.registry import DimensionRegistry
from temporal_tree.data.dimensions.gas_meter import MeterGasDimension
from temporal_tree.data.dimensions.gas_standard import StandardGasDimension
from temporal_tree.data.dimensions.loss_rate import LossRateDimension
from temporal_tree.core.time.timeline import Timeline
from temporal_tree.core.time.snapshot import SnapshotSystem
from temporal_tree.services.import_export.excel_importer import GasExcelImporter
from temporal_tree.exceptions import DimensionError, NodeError
from temporal_tree.config.settings import SystemSettings

print("=" * 70)
print("🌳 ntreemode 燃气输差分析系统 - 完整使用流程")
print("=" * 70)

def main():
    """
主函数
"""

    # --- 1. 初始化系统 ---
    print("\n[1/9] 初始化系统组件...")

    # 创建数据目录
    os.makedirs("./data", exist_ok=True)
    os.makedirs("./exports", exist_ok=True)
    os.makedirs("./snapshots", exist_ok=True)

    # 配置
    settings = SystemSettings(
        max_tree_depth=10,
        max_children_per_node=100,
        storage_backend="sqlite",
        storage_path="./data/gas_system.db"
    )

    # 组件装配
    ip_provider = IncrementalIPProvider(
        max_depth=settings.max_tree_depth,
        max_children_per_node=settings.max_children_per_node
    )

    factory = NodeFactory(ip_provider)

    # 注册维度
    dimension_registry = DimensionRegistry()
    dimension_registry.register(MeterGasDimension())
    dimension_registry.register(StandardGasDimension())
    dimension_registry.register(LossRateDimension())

    print("  ✅ 组件初始化完成")
    print(f"  📁 存储路径: {settings.storage_path}")
    print(f"  📏 已注册维度: {[d.name for d in dimension_registry.list_dimensions()]}")

    # --- 2. 构建组织架构树 ---
    print("\n[2/9] 构建组织架构树...")

    # 创建根节点（集团总部）
    root = factory.create_root_node("北京燃气集团")
    root.dimension_registry = dimension_registry
    root.add_tag("headquarter")
    root.add_tag("gas_company")
    print(f"  ✅ 创建根节点: {root.name} (IP: {root.ip})")

    # 创建省级公司
    beijing = factory.create_child_node(root, "北京市公司")
    beijing.dimension_registry = dimension_registry
    beijing.add_tag("city_level")
    beijing.metadata["region"] = "北京"
    beijing.metadata["established"] = "2000-01-01"

    tianjin = factory.create_child_node(root, "天津市公司")
    tianjin.dimension_registry = dimension_registry
    tianjin.add_tag("city_level")
    tianjin.metadata["region"] = "天津"

    hebei = factory.create_child_node(root, "河北省公司")
    hebei.dimension_registry = dimension_registry
    hebei.add_tag("city_level")

    print(f"  ✅ 创建省级公司: {beijing.name}, {tianjin.name}, {hebei.name}")

    # 北京市公司的下属单位
    chaoyang = factory.create_child_node(beijing, "朝阳分公司")
    chaoyang.dimension_registry = dimension_registry
    chaoyang.add_tag("district_level")
    chaoyang.metadata["area_code"] = "010"
    chaoyang.metadata["customer_count"] = 150000

    haidian = factory.create_child_node(beijing, "海淀分公司")
    haidian.dimension_registry = dimension_registry
    haidian.add_tag("district_level")
    haidian.metadata["area_code"] = "010"
    haidian.metadata["customer_count"] = 180000

    # 朝阳分公司下的具体站点
    station_a = factory.create_child_node(chaoyang, "北苑站")
    station_a.dimension_registry = dimension_registry
    station_a.add_tag("station")
    station_a.metadata["capacity"] = 5000
    station_a.metadata["device_id"] = "ST-001"

    station_b = factory.create_child_node(chaoyang, "望京站")
    station_b.dimension_registry = dimension_registry
    station_b.add_tag("station")
    station_b.metadata["capacity"] = 8000
    station_b.metadata["device_id"] = "ST-002"

    print(f"  ✅ 创建站点: {station_a.name}, {station_b.name}")

    # --- 3. 保存树结构到存储 ---
    tree_id = "gas_tree_2024"

    # 保存树元数据
    storage.save_tree(tree_id, {
        "name": "北京燃气集团",
        "created_at": datetime.now().isoformat(),
        "root_ip": root.ip,
        "node_count": 0,  # 稍后更新
        "settings": settings.__dict__
    })

    # 保存所有节点
    all_nodes = [root, beijing, tianjin, hebei, chaoyang, haidian, station_a, station_b]
    for node in all_nodes:
        storage.save_node(tree_id, node.to_dict())

    # 更新节点计数
    storage.update_tree_meta(tree_id, {"node_count": len(all_nodes)})

    print(f"  ✅ 已保存 {len(all_nodes)} 个节点到存储")

    # --- 4. 记录历史气量数据（模拟90天）---
    print("\n[3/9] 记录历史气量数据（模拟90天）...")

    start_date = datetime(2024, 1, 1)
    data_points = 0

    for day in range(90):
        current_date = start_date + timedelta(days=day)

        # 基础气量（带季节性波动）
        seasonal_factor = 1.0 + 0.2 * (current_date.month in [1, 2, 12])  # 冬季用气多

        # 海淀分公司数据
        base_meter_hd = 2500 + random.randint(-200, 200) * seasonal_factor
        base_standard_hd = 2600 + random.randint(-150, 150) * seasonal_factor

        try:
            haidian.set_data("meter_gas", round(base_meter_hd, 2), current_date, 
                           {"source": "自动采集", "device": "meter_01"})
            haidian.set_data("standard_gas", round(base_standard_hd, 2), current_date,
                           {"source": "自动采集", "device": "meter_01"})
            data_points += 2
        except DimensionError as e:
            print(f"     ⚠️ 海淀数据记录失败: {e}")

        # 朝阳分公司数据
        base_meter_cy = 3750 + random.randint(-300, 300) * seasonal_factor
        base_standard_cy = 3900 + random.randint(-250, 250) * seasonal_factor

        try:
            chaoyang.set_data("meter_gas", round(base_meter_cy, 2), current_date,
                            {"source": "自动采集", "device": "meter_02"})
            chaoyang.set_data("standard_gas", round(base_standard_cy, 2), current_date,
                            {"source": "自动采集", "device": "meter_02"})
            data_points += 2
        except DimensionError as e:
            print(f"     ⚠️ 朝阳数据记录失败: {e}")

        # 站点数据
        try:
            station_a.set_data("meter_gas", round(base_meter_cy * 0.3, 2), current_date)
            station_a.set_data("standard_gas", round(base_standard_cy * 0.3, 2), current_date)
            station_b.set_data("meter_gas", round(base_meter_cy * 0.4, 2), current_date)
            station_b.set_data("standard_gas", round(base_standard_cy * 0.4, 2), current_date)
            data_points += 4
        except DimensionError as e:
            print(f"     ⚠️ 站点数据记录失败: {e}")

        # 每10天打印进度
        if (day + 1) % 30 == 0:
            print(f"     📊 已记录 {(day + 1)} 天数据...")

    print(f"  ✅ 已记录 {data_points} 个数据点")
    print(f"  💾 数据已自动持久化到SQLite数据库")

    # --- 5. 计算输差率（自动计算衍生维度）---
    print("\n[4/9] 计算输差率分析...")

    loss_dimension = dimension_registry.get_dimension("loss_rate")
    alert_count = {"green": 0, "orange": 0, "red": 0}

    # 为海淀分公司计算输差率
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    meter_data = haidian.get_time_series("meter_gas", start_date, end_date)
    standard_data = haidian.get_time_series("standard_gas", start_date, end_date)

    # 创建时间到值的映射
    meter_dict = {ts: val for ts, val in meter_data}
    standard_dict = {ts: val for ts, val in standard_data}

    for timestamp in meter_dict.keys():
        if timestamp in standard_dict:
            loss_data = {
                "meter": meter_dict[timestamp],
                "standard": standard_dict[timestamp]
            }

            try:
                loss_rate = loss_dimension.calculate(loss_data)
                alert_level = loss_dimension.get_alert_level(loss_rate)

                # 记录输差率
                haidian.set_data("loss_rate", round(loss_rate, 4), timestamp,
                               {"meter_value": meter_dict[timestamp], 
                                "standard_value": standard_dict[timestamp]})

                # 统计告警级别
                if alert_level == "正常":
                    alert_count["green"] += 1
                elif alert_level == "警告":
                    alert_count["orange"] += 1
                else:
                    alert_count["red"] += 1

            except Exception as e:
                print(f"     ⚠️ 输差率计算失败: {e}")

    print(f"  📈 输差率分析结果（海淀分公司）:")
    print(f"     ✅ 正常: {alert_count['green']} 天")
    print(f"     ⚠️ 警告: {alert_count['orange']} 天")
    print(f"     🚨 报警: {alert_count['red']} 天")

    if alert_count['red'] > 0:
        print(f"     💡 建议: 存在严重输差，请检查计量设备！")

    # --- 6. 创建快照 ---
    print("\n[5/9] 创建系统快照...")

    snapshot_system = SnapshotSystem(storage)

    # 为单个节点创建快照
    snapshot_id_1 = snapshot_system.create_node_snapshot(
        haidian, 
        "haidian_2024_q1", 
        metadata={
            "reason": "季度结算", 
            "operator": "admin",
            "timestamp": datetime.now().isoformat()
        }
    )
    print(f"  ✅ 创建节点快照: {snapshot_id_1}")

    # 为整棵树创建快照
    snapshot_id_2 = snapshot_system.create_tree_snapshot(
        root, 
        "beijing_gas_2024_q1", 
        metadata={
            "quarter": "Q1", 
            "year": 2024,
            "company": "北京燃气集团",
            "node_count": len(all_nodes)
        }
    )
    print(f"  ✅ 创建整树快照: {snapshot_id_2}")

    # 查看快照历史
    snapshots = snapshot_system.get_node_snapshots(haidian.node_id)
    print(f"  📸 海淀分公司共有 {len(snapshots)} 个历史快照")

    # --- 7. 查询与分析 ---
    print("\n[6/9] 数据查询与分析...")

    repo = NodeRepository(root)

    # 查询所有站点节点
    stations = repo.find_nodes(tags=["station"])
    print(f"  🔍 找到 {len(stations)} 个站点:")

    for station in stations:
        print(f"     - {station.name} (IP: {station.ip})")
        print(f"       设备ID: {station.metadata.get('device_id', 'N/A')}")

        # 获取最近7天气量数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        meter_series = station.get_time_series("meter_gas", start_date, end_date)

        if meter_series:
            avg_meter = sum(v for _, v in meter_series) / len(meter_series)
            max_meter = max(v for _, v in meter_series)
            min_meter = min(v for _, v in meter_series)
            print(f"        📊 最近7日气量: 平均 {avg_meter:.1f} m³, "
                  f"最大 {max_meter:.1f} m³, 最小 {min_meter:.1f} m³")

    # 查询指定区域的所有节点
    beijing_nodes = repo.find_nodes(metadata={"region": "北京"})
    print(f"\n  🏢 北京区域节点数: {len(beijing_nodes)}")

    # 获取树深度
    tree_depth = repo.get_tree_depth()
    print(f"  📐 树深度: {tree_depth} 层")

    # --- 8. 时间线数据验证 ---
    print("\n[7/9] 验证时间线持久化...")

    # 从存储重新加载节点，验证数据是否持久化
    print("  正在从SQLite重新加载数据...")

    # 创建新节点实例，从存储加载数据
    reloaded_haidian = None
    node_data = storage.get_node(tree_id, haidian.node_id)
    if node_data:
        from temporal_tree.core.node.entity import TreeNode
        reloaded_haidian = TreeNode.from_dict(
            node_data, 
            storage=storage,
            dimension_registry=dimension_registry
        )

    if reloaded_haidian:
        # 验证最新数据
        latest_meter = reloaded_haidian.get_data("meter_gas")
        latest_standard = reloaded_haidian.get_data("standard_gas")
        latest_loss = reloaded_haidian.get_data("loss_rate")

        print(f"  ✅ 数据恢复成功:")
        print(f"     - 最新表计气量: {latest_meter} m³")
        print(f"     - 最新标准气量: {latest_standard} m³")
        print(f"     - 最新输差率: {latest_loss:.2%}" if latest_loss else "     - 无输差率数据")

        # 验证时间序列
        series = reloaded_haidian.get_time_series("meter_gas", 
                                                 datetime(2024, 1, 1), 
                                                 datetime(2024, 1, 10))
        print(f"     - 2024年1月上旬数据点: {len(series)} 个")
    else:
        print("  ⚠️ 无法从存储恢复节点数据")

    # --- 9. 导入导出功能演示 ---
    print("\n[8/9] Excel导入导出功能...")

    # 初始化导入器
    importer = GasExcelImporter(dimension_registry, storage)

    # 创建示例Excel数据
    print("  正在生成示例Excel数据...")
    excel_data = create_sample_excel()

    # 导入到朝阳分公司
    try:
        import_result = importer.import_from_excel(
            excel_data,
            target_node=chaoyang,
            date_column="日期",
            value_columns=["meter_gas", "standard_gas"],
            date_format="%Y-%m-%d"
        )

        print(f"  ✅ Excel导入完成:")
        print(f"     - 导入记录数: {import_result['imported_count']}")
        print(f"     - 新增数据点: {import_result.get('stats', {}).get('total_points', 0)}")
    except Exception as e:
        print(f"  ⚠️ Excel导入失败（可忽略）: {e}")

    # --- 10. 系统状态摘要 ---
    print("\n[9/9] 系统状态摘要")
    print("=" * 70)
    print(f"🌳 树ID: {tree_id}")
    print(f"📊 节点总数: {len(all_nodes)}")
    print(f"📏 树深度: {tree_depth}")
    print(f"💾 存储类型: {storage.__class__.__name__}")
    print(f"📈 总数据点: ~{data_points}")
    print(f"📸 快照数量: 2")
    print("\n📋 节点清单:")

    # 打印树结构
    from examples.visualization import TreeVisualizer
    TreeVisualizer.console_print(root, show_ip=True, show_data=True)

    print("\n" + "=" * 70)
    print("🎉 完整使用流程执行成功！")
    print("=" * 70)

    return {
        "tree_id": tree_id,
        "root": root,
        "storage": storage,
        "stats": {
            "node_count": len(all_nodes),
            "tree_depth": tree_depth,
            "data_points": data_points,
            "snapshots": 2
        }
    }


def create_sample_excel():
    """
创建示例Excel数据
"""
    import pandas as pd
    from io import BytesIO

    data = {
        "日期": ["2024-02-01", "2024-02-02", "2024-02-03", "2024-02-04", "2024-02-05"],
        "节点名称": ["朝阳分公司", "朝阳分公司", "朝阳分公司", "朝阳分公司", "朝阳分公司"],
        "meter_gas": [3750.5, 3820.3, 3680.8, 3910.2, 3850.6],
        "standard_gas": [3900.2, 3980.1, 3820.5, 4080.3, 4010.9],
        "操作员": ["张三", "张三", "李四", "王五", "张三"],
        "备注": ["正常", "正常", "波动", "正常", "正常"]
    }

    df = pd.DataFrame(data)
    output = BytesIO()

    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='气量数据', index=False)
    except:
        # 如果没有openpyxl，使用xlsxwriter
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='气量数据', index=False)

    return output.getvalue()


if __name__ == "__main__":
    try:
        result = main()
        print("\n💡 下一步建议:")
        print("  1. 运行 examples/visualization.py 查看可视化树形图")
        print("  2. 检查 ./data/gas_system.db 确认数据持久化")
        print("  3. 尝试时间旅行功能（待实现）")
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断执行")
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()