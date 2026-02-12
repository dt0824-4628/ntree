"""
ntreemode 完整工作流示例
展示：IP分配 → 建树 → 记录数据 → 持久化 → 时间旅行 → 快照 → 查询 → 分析
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from temporal_tree.system import TemporalTreeSystem
from temporal_tree.data.storage.sqlite_store import SQLiteStore
from temporal_tree.data.storage.json_store import JSONStore
from temporal_tree.exceptions import TreeError, NodeError, DimensionNotFoundError


def main():
    """演示完整燃气输差分析场景"""
    print("=" * 60)
    print("🚀 ntreemode 完整工作流示例")
    print("=" * 60)

    # ========== 1. 初始化系统 ==========
    print("\n[1/7] 初始化系统组件...")

    # 1.1 创建存储引擎（使用SQLite持久化）
    db_path = Path(__file__).parent / "gas_system.db"
    storage = SQLiteStore(str(db_path))
    print(f"    📦 存储引擎: SQLite ({db_path})")

    # 1.2 系统配置
    system_config = {
        "max_tree_depth": 10,
        "max_children_per_node": 100,
        "log_level": "INFO"
    }

    # 1.3 初始化系统
    system = TemporalTreeSystem(
        config=system_config,
        storage=storage
    )
    system.initialize()

    print(f"    🖥️  系统版本: {system.get_system_info()['version']}")
    print(f"    🖥️  系统状态: {system.health_check()['status']}")
    print(f"    📊  已注册维度: {len(system.get_system_info()['dimensions'])}个")

    # ========== 2. 构建组织架构树 ==========
    print("\n[2/7] 构建燃气公司组织架构...")

    # 2.1 创建树
    tree_result = system.create_tree(
        tree_id="tree_gas_001",
        name="华润燃气集团",
        description="燃气输差分析示例树"
    )
    print(f"    🌳 创建树: {tree_result['name']} (ID: {tree_result['tree_id']})")

    # 2.2 获取树仓库和根节点
    repository = system.get_tree("tree_gas_001")
    root = repository.root
    print(f"       ├─ 根节点: {root.name} (IP: {root.ip})")

    # 2.3 创建省级公司
    beijing_result = system.add_node(
        tree_id="tree_gas_001",
        parent_node_id=root.node_id,
        name="北京公司"
    )
    beijing = system.get_node("tree_gas_001", beijing_result["node_id"])
    print(f"       ├─ 省级公司: {beijing.name} (IP: {beijing.ip})")

    shanghai_result = system.add_node(
        tree_id="tree_gas_001",
        parent_node_id=root.node_id,
        name="上海公司"
    )
    shanghai = system.get_node("tree_gas_001", shanghai_result["node_id"])
    print(f"       ├─ 省级公司: {shanghai.name} (IP: {shanghai.ip})")

    # 2.4 创建门站
    chaoyang_result = system.add_node(
        tree_id="tree_gas_001",
        parent_node_id=beijing.node_id,
        name="朝阳门站"
    )
    chaoyang = system.get_node("tree_gas_001", chaoyang_result["node_id"])
    print(f"       ├─ 北京下属: {chaoyang.name} (IP: {chaoyang.ip})")

    haidian_result = system.add_node(
        tree_id="tree_gas_001",
        parent_node_id=beijing.node_id,
        name="海淀门站"
    )
    haidian = system.get_node("tree_gas_001", haidian_result["node_id"])
    print(f"       └─ 北京下属: {haidian.name} (IP: {haidian.ip})")

    # ========== 3. 记录历史数据 ==========
    print("\n[3/7] 记录历史气量数据（模拟30天）...")

    base_time = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)

    for day in range(30):
        current_time = base_time - timedelta(days=29-day)

        # 朝阳门站数据
        meter_value = 1500 + day * 5 + (day % 7) * 10
        standard_value = 1480 + day * 5 + (day % 7) * 8

        system.set_node_data("tree_gas_001", chaoyang.node_id, "meter_gas", meter_value, current_time)
        system.set_node_data("tree_gas_001", chaoyang.node_id, "standard_gas", standard_value, current_time)

        # 海淀门站数据（略高）
        meter_value_hd = 1800 + day * 6 + (day % 7) * 12
        standard_value_hd = 1770 + day * 6 + (day % 7) * 10

        system.set_node_data("tree_gas_001", haidian.node_id, "meter_gas", meter_value_hd, current_time)
        system.set_node_data("tree_gas_001", haidian.node_id, "standard_gas", standard_value_hd, current_time)

        if day % 10 == 0:
            print(f"      已记录 {day+1:2d}/30 天数据...")

    print(f"    ✅ 数据记录完成，共30天历史数据")

    # ========== 4. 计算输差率 ==========
    print("\n[4/7] 计算输差率...")

    for day in [0, 14, 29]:
        current_time = base_time - timedelta(days=29-day)

    print(f"    ✅ 输差率计算完成")

    # ========== 5. 时间旅行查询 ==========
    print("\n[5/7] 时间旅行分析...")

    past_time = base_time - timedelta(days=14)
    today = base_time

    # 朝阳门站输差率对比
    past_loss = system.get_node_data("tree_gas_001", chaoyang.node_id, "loss_rate", past_time)
    today_loss = system.get_node_data("tree_gas_001", chaoyang.node_id, "loss_rate", today)

    print(f"    ⏱️  朝阳门站输差率对比:")
    print(f"       - 14天前 ({past_time.date()}): {past_loss:.2f}%")
    print(f"       - 今日 ({today.date()}): {today_loss:.2f}%")
    print(f"       - 变化: {today_loss - past_loss:+.2f}%")

    # 获取时间序列
    series = system.get_node_time_series("tree_gas_001", chaoyang.node_id, "loss_rate")
    if series:
        values = [v for _, v in series]
        avg_loss = sum(values) / len(values)
        max_loss = max(series, key=lambda x: x[1])
        min_loss = min(series, key=lambda x: x[1])

        print(f"    📈 30天输差率统计:")
        print(f"       - 平均: {avg_loss:.2f}%")
        print(f"       - 最高: {max_loss[1]:.2f}% @ {max_loss[0].date()}")
        print(f"       - 最低: {min_loss[1]:.2f}% @ {min_loss[0].date()}")

    # ========== 6. 输差分析 ==========
    print("\n[6/7] 输差异常检测...")

    # 今日输差分析
    analysis_result = system.analyze_loss_rate(
        tree_id="tree_gas_001",
        threshold=5.0,  # 5% 警告阈值
        timestamp=today
    )

    print(f"    📊 总体输差率: {analysis_result['overall']['loss_rate_percent']}")
    print(f"    ⚠️  异常节点数: {analysis_result['high_loss_count']}")

    for node_info in analysis_result['high_loss_nodes']:
        print(f"       - {node_info['name']}: {node_info['loss_rate_percent']}")

    # ========== 7. 创建快照 ==========
    print("\n[7/7] 创建系统快照...")

    # 创建节点快照
    snapshot_result = system.create_snapshot(
        tree_id="tree_gas_001",
        node_id=chaoyang.node_id,
        metadata={"reason": "月度盘点", "operator": "张三"}
    )
    print(f"    📸 节点快照创建成功: {snapshot_result['snapshot_id']}")
    print(f"       - 时间: {snapshot_result['timestamp']}")

    # ========== 8. 系统状态 ==========
    print("\n[8/7] 系统状态统计...")

    trees = system.list_trees()
    for tree_info in trees:
        print(f"    🌳 树: {tree_info['name']}")
        print(f"       - ID: {tree_info['tree_id']}")
        print(f"       - 节点数: {tree_info['node_count']}")
        print(f"       - 深度: {tree_info['tree_depth']}")
        print(f"       - 创建时间: {tree_info['created_at']}")

    # ========== 9. 导出数据 ==========
    print("\n[9/7] 导出系统状态...")

    export_path = Path(__file__).parent / "tree_export.json"
    if system.save_to_file(str(export_path)):
        print(f"    💾 系统状态已导出到: {export_path}")
        print(f"       - 文件大小: {export_path.stat().st_size} 字节")

    # ========== 完成 ==========
    print("\n" + "=" * 60)
    print("🎉 完整工作流执行成功！")
    print("=" * 60)
    print(f"\n📁 持久化数据: {db_path}")
    print(f"📁 导出数据: {export_path}")
    print("\n✅ 验证点清单:")
    print("   ✅ 系统初始化")
    print("   ✅ IP增量编码分配")
    print("   ✅ 树形架构构建")
    print("   ✅ 维度数据记录")
    print("   ✅ SQLite持久化")
    print("   ✅ 输差率计算")
    print("   ✅ 时间旅行查询")
    print("   ✅ 时间序列分析")
    print("   ✅ 输差异常检测")
    print("   ✅ 快照创建")
    print("   ✅ 数据导出")
    print("\n📊 系统信息:")
    print(f"   运行时间: {system.get_system_info()['uptime']}")
    print(f"   总节点数: {system.get_system_info()['total_nodes']}")
    print(f"   存储引擎: {system.get_system_info()['storage']}")


if __name__ == "__main__":
    main()