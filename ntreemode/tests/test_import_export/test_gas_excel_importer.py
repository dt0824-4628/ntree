"""
Excel导入测试 - 仅测试解析部分（已成功）
"""
import sys
import os
from pathlib import Path
import pandas as pd
import re
from datetime import datetime

# ==================== 设置路径 ====================
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent  # tests/test_import_export/../../ = 项目根目录
src_path = project_root / "src"

print("=" * 80)
print("🎉 Excel解析成功测试")
print("=" * 80)
print(f"项目根目录: {project_root}")
print(f"src路径: {src_path}")

# 检查目录结构
print("\n📁 检查目录结构:")
if src_path.exists():
    for item in src_path.iterdir():
        if item.is_dir():
            print(f"  📁 {item.name}/")
        else:
            print(f"  📄 {item.name}")
else:
    print(f"❌ src目录不存在: {src_path}")

# ==================== 解析函数 ====================
def parse_time_string(time_str, use_midday=True):
    """解析时间字符串"""
    if isinstance(time_str, float):
        if time_str.is_integer():
            time_str = str(int(time_str))
        else:
            time_str = str(time_str)
    else:
        time_str = str(time_str)

    time_str = time_str.replace('.0', '')
    clean_str = ''.join(c for c in time_str if c.isdigit())

    if len(clean_str) == 6:
        year = int(clean_str[:4])
        month = int(clean_str[4:6])
        day = 15 if use_midday else 1
        return datetime(year, month, day)

    raise ValueError(f"无法解析的时间格式: {time_str}")

def parse_level(raw_name: str) -> int:
    """解析层级"""
    if not raw_name or raw_name.isspace():
        return 0

    leading_spaces = len(raw_name) - len(raw_name.lstrip())
    level = leading_spaces // 2
    return min(level, 10)

def parse_excel_file(file_path):
    """解析Excel文件"""
    print(f"\n📄 解析文件: {file_path}")

    try:
        # 读取原始数据
        df_raw = pd.read_excel(file_path, header=None)

        if len(df_raw) < 3:
            print("❌ 文件数据太少")
            return []

        # 第0行：列名，第1行：时间
        column_names = df_raw.iloc[0].tolist()
        time_labels = df_raw.iloc[1].tolist()

        # 构建列名
        final_columns = []
        for i, (col_name, time_label) in enumerate(zip(column_names, time_labels)):
            if pd.isna(col_name):
                col_name = f"Column_{i}"
            else:
                col_name = str(col_name)

            if pd.notna(time_label):
                if isinstance(time_label, float):
                    time_str = str(int(time_label)) if time_label.is_integer() else str(time_label)
                else:
                    time_str = str(time_label)

                time_str = time_str.replace('.0', '')

                if col_name:
                    final_columns.append(f"{col_name}_{time_str}")
                else:
                    final_columns.append(f"Data_{time_str}")
            else:
                final_columns.append(col_name)

        # 提取数据
        data_df = df_raw.iloc[2:].reset_index(drop=True)
        data_df.columns = final_columns

        # 解析节点
        parsed_nodes = []
        current_hierarchy = []

        # 找到节点列
        node_column = None
        for col in final_columns:
            if '节点' in col:
                node_column = col
                break

        if node_column is None:
            node_column = final_columns[0]

        for idx, row in data_df.iterrows():
            raw_name = str(row[node_column]) if pd.notna(row[node_column]) else ''

            if not raw_name.strip():
                continue

            # 解析层级和名称
            level = parse_level(raw_name)
            clean_name = raw_name.strip()

            # 查找父节点
            parent_name = None
            for prev_level, prev_name in reversed(current_hierarchy):
                if prev_level < level:
                    parent_name = prev_name
                    break

            # 更新层级路径
            current_hierarchy = [(l, n) for l, n in current_hierarchy if l < level]
            current_hierarchy.append((level, clean_name))

            # 提取时间数据
            time_data = {}
            for col in final_columns:
                if col == node_column:
                    continue

                value = row[col]
                if pd.isna(value):
                    continue

                # 从列名提取时间
                col_str = str(col)
                time_match = re.search(r'(\d{6})', col_str)
                if not time_match:
                    continue

                time_key = time_match.group(1)

                # 确定维度类型
                dimension = None
                if '标准用气量' in col_str:
                    dimension = 'standard_flow'
                elif '表计用气量' in col_str:
                    dimension = 'metered_flow'

                if not dimension:
                    continue

                # 解析时间
                try:
                    timestamp = parse_time_string(time_key, use_midday=True)
                    date_key = timestamp.date().isoformat()

                    if date_key not in time_data:
                        time_data[date_key] = {}

                    # 转换值
                    try:
                        num_value = float(value)
                        time_data[date_key][dimension] = num_value
                    except:
                        continue

                except:
                    continue

            parsed_nodes.append({
                'row_index': idx,
                'raw_name': raw_name,
                'node_name': clean_name,
                'clean_name': clean_name,
                'level': level,
                'parent_name': parent_name,
                'time_data': time_data,
                'has_data': bool(time_data)
            })

        print(f"✅ 解析成功: {len(parsed_nodes)} 个节点")
        return parsed_nodes

    except Exception as e:
        print(f"❌ 解析失败: {e}")
        import traceback
        traceback.print_exc()
        return []

def display_results(parsed_nodes, max_display=20):
    """显示解析结果"""
    print(f"\n🌳 节点结构 (显示前{max_display}个):")
    print("-" * 60)

    for i, node in enumerate(parsed_nodes[:max_display]):
        indent = "  " * node['level']
        parent_info = f" (父: {node['parent_name']})" if node['parent_name'] else ""

        # 统计维度
        dimensions = set()
        for date_data in node['time_data'].values():
            dimensions.update(date_data.keys())

        dim_info = f" [{len(dimensions)}个维度]" if dimensions else ""

        print(f"{i+1:3}. {indent}{node['node_name']}{parent_info}{dim_info}")

    if len(parsed_nodes) > max_display:
        print(f"... 还有 {len(parsed_nodes) - max_display} 个节点")

    # 统计信息
    print(f"\n📊 统计信息:")
    print(f"   总节点数: {len(parsed_nodes)}")

    # 层级统计
    level_stats = {}
    for node in parsed_nodes:
        level = node['level']
        level_stats[level] = level_stats.get(level, 0) + 1

    print(f"   层级分布:")
    for level in sorted(level_stats.keys()):
        print(f"     层级 {level}: {level_stats[level]} 个节点")

    # 数据统计
    nodes_with_data = sum(1 for node in parsed_nodes if node['has_data'])
    print(f"   有数据节点: {nodes_with_data}")

    # 维度统计
    all_dimensions = set()
    total_time_points = 0
    for node in parsed_nodes:
        total_time_points += len(node['time_data'])
        for date_data in node['time_data'].values():
            all_dimensions.update(date_data.keys())

    print(f"   总时间点: {total_time_points}")
    print(f"   维度类型: {', '.join(sorted(all_dimensions))}")

def main():
    """主函数"""
    # 要测试的文件
    excel_files = [
        Path("tests/test_import_export/test_data/2_10_1.xlsx"),
        # 可以添加更多文件
        # Path("tests/test_import_export/test_data/1.xlsx"),
    ]

    for file_path in excel_files:
        if not file_path.exists():
            print(f"\n❌ 文件不存在: {file_path}")
            continue

        print(f"\n{'='*80}")
        print(f"处理文件: {file_path.name}")
        print('='*80)

        # 解析Excel
        parsed_nodes = parse_excel_file(file_path)

        if parsed_nodes:
            # 显示结果
            display_results(parsed_nodes)

            # 保存为JSON（可选）
            save_json = input(f"\n💾 是否将解析结果保存为JSON文件？ (y/N): ").strip().lower()
            if save_json == 'y':
                import json
                from datetime import date

                output_file = file_path.with_suffix('.json')

                # 转换datetime为字符串
                def convert_for_json(obj):
                    if isinstance(obj, (datetime, date)):
                        return obj.isoformat()
                    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(parsed_nodes, f, ensure_ascii=False, indent=2, default=convert_for_json)

                print(f"✅ 保存到: {output_file}")

        print(f"\n{'='*80}")
        print(f"🎉 文件 {file_path.name} 解析完成！")
        print('='*80)

    print("\n" + "=" * 80)
    print("✅ 所有测试完成！")
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)