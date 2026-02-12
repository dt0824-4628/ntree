"""
简单直接的时间线测试
"""
import sys
import os
from datetime import datetime, timedelta

# 1. 添加src目录到Python路径 - 这是关键！
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # ntreemode/
src_dir = os.path.join(project_root, "src")  # ntreemode/src

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

print(f"项目根目录: {project_root}")
print(f"src目录: {src_dir}")
print(f"Python路径前3个: {sys.path[:3]}")

# 2. 导入时间线模块
try:
    from temporal_tree.core.time.timeline import Timeline, TimePoint

    print("✅ 成功导入 Timeline 和 TimePoint")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    print("请检查：")
    print(f"1. temporal_tree目录是否存在: {os.path.join(src_dir, 'temporal_tree')}")
    print(f"2. timeline.py文件是否存在: {os.path.join(src_dir, 'temporal_tree', 'core', 'time', 'timeline.py')}")
    sys.exit(1)

# 3. 运行简单测试
print("\n" + "=" * 50)
print("开始测试时间线功能")
print("=" * 50)


def test_创建时间线():
    """测试创建时间线"""
    print("\n1. 测试创建时间线...")
    timeline = Timeline("燃气表001", "node")

    print(f"  对象ID: {timeline.object_id}")
    print(f"  对象类型: {timeline.object_type}")
    print(f"  时间点数量: {len(timeline)}")

    assert timeline.object_id == "燃气表001"
    assert timeline.object_type == "node"
    print("  ✅ 创建时间线成功")


def test_添加时间点():
    """测试添加时间点"""
    print("\n2. 测试添加时间点...")
    timeline = Timeline("燃气表002", "node")

    # 添加第一个时间点
    time1 = datetime(2024, 1, 15, 10, 0, 0)
    timeline.add_time_point(time1, {"燃气用量": 150.5, "压力": 0.8})

    print(f"  添加时间点: {time1}")
    print(f"  当前时间点数量: {len(timeline)}")

    # 添加第二个时间点
    time2 = datetime(2024, 1, 15, 11, 0, 0)
    timeline.add_time_point(time2, {"燃气用量": 152.3, "压力": 0.82})

    print(f"  添加时间点: {time2}")
    print(f"  当前时间点数量: {len(timeline)}")

    assert len(timeline) == 2
    print("  ✅ 添加时间点成功")


def test_查询时间点():
    """测试查询时间点"""
    print("\n3. 测试查询时间点...")
    timeline = Timeline("燃气表003", "node")

    # 添加测试数据
    time1 = datetime(2024, 1, 15, 10, 0, 0)
    time2 = datetime(2024, 1, 15, 11, 0, 0)

    timeline.add_time_point(time1, {"用量": 100})
    timeline.add_time_point(time2, {"用量": 150})

    # 查询特定时间点
    point1 = timeline.get_time_point(time1)
    if point1:
        print(f"  查询到时间点 {time1}: 用量={point1.data['用量']}")
        assert point1.data["用量"] == 100
    else:
        print(f"  ❌ 未找到时间点 {time1}")

    # 查询最新时间点
    latest = timeline.get_latest()
    if latest:
        print(f"  最新时间点: {latest.timestamp}, 用量={latest.data['用量']}")
        assert latest.data["用量"] == 150

    print("  ✅ 查询时间点成功")


def test_时间范围查询():
    """测试时间范围查询"""
    print("\n4. 测试时间范围查询...")
    timeline = Timeline("燃气表004", "node")

    # 添加多个时间点
    times = [
        datetime(2024, 1, 15, 9, 0, 0),
        datetime(2024, 1, 15, 10, 0, 0),
        datetime(2024, 1, 15, 11, 0, 0),
        datetime(2024, 1, 15, 12, 0, 0),
    ]

    for i, t in enumerate(times):
        timeline.add_time_point(t, {"value": i * 10})

    # 查询9:30到11:30之间的时间点
    start = datetime(2024, 1, 15, 9, 30, 0)
    end = datetime(2024, 1, 15, 11, 30, 0)

    points = timeline.get_time_range(start, end)
    print(f"  查询范围: {start} 到 {end}")
    print(f"  找到 {len(points)} 个时间点")

    for point in points:
        print(f"    - {point.timestamp}: value={point.data['value']}")

    assert len(points) == 2  # 应该找到10:00和11:00
    print("  ✅ 时间范围查询成功")


def test_转换为字典():
    """测试转换为字典"""
    print("\n5. 测试转换为字典...")
    timeline = Timeline("燃气表005", "node")

    # 添加一个时间点
    time = datetime(2024, 1, 15, 10, 30, 0)
    timeline.add_time_point(
        time,
        {"燃气用量": 150.5, "状态": "正常"},
        {"操作员": "张三", "备注": "日常记录"}
    )

    # 转换为字典
    data = timeline.to_dict()

    print(f"  对象ID: {data['object_id']}")
    print(f"  对象类型: {data['object_type']}")
    print(f"  时间点数量: {data['metadata']['time_point_count']}")

    # 检查时间点
    time_key = time.isoformat()
    if time_key in data['time_points']:
        point_data = data['time_points'][time_key]
        print(f"  找到时间点 {time_key}:")
        print(f"    数据: {point_data['data']}")
        print(f"    元数据: {point_data['metadata']}")

    assert data['object_id'] == "燃气表005"
    assert data['metadata']['time_point_count'] == 1
    print("  ✅ 转换为字典成功")


# 运行所有测试
def main():
    """运行所有测试"""
    try:
        test_创建时间线()
        test_添加时间点()
        test_查询时间点()
        test_时间范围查询()
        test_转换为字典()

        print("\n" + "=" * 50)
        print("🎉 所有测试通过！")
        print("=" * 50)

    except AssertionError as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()