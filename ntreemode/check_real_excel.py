"""
正确解析实际Excel文件的测试程序
"""
import sys
from pathlib import Path
import pandas as pd
import re

# 添加src到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

print("=" * 80)
print("正确解析实际Excel文件")
print("=" * 80)

def analyze_excel_structure(file_path):
    """分析Excel文件的实际结构"""
    print(f"\n🔍 分析文件: {file_path}")

    try:
        # 1. 用不同方式读取，查看实际结构
        print("\n1. 读取原始数据（无表头）:")
        df_raw = pd.read_excel(file_path, header=None, nrows=10)
        print(f"   形状: {df_raw.shape}")
        print(f"   前3行:")
        for i in range(min(3, len(df_raw))):
            print(f"     行{i}: {df_raw.iloc[i].tolist()[:10]}")  # 只显示前10列

        # 2. 检查哪些行可能是表头
        print("\n2. 分析表头行:")
        for i in range(min(5, len(df_raw))):
            row = df_raw.iloc[i]
            # 检查是否包含"节点"、"标准"、"表计"等关键词
            has_node = any(isinstance(cell, str) and '节点' in str(cell) for cell in row)
            has_standard = any(isinstance(cell, str) and '标准' in str(cell) for cell in row)
            has_metered = any(isinstance(cell, str) and '表计' in str(cell) for cell in row)
            has_time = any(isinstance(cell, (int, float)) and 200000 < cell < 210000 for cell in row if pd.notna(cell))

            print(f"   行{i}: 有节点={has_node}, 有标准={has_standard}, 有表计={has_metered}, 有时间={has_time}")

        # 3. 正确的读取方式
        print("\n3. 尝试正确读取:")

        # 方式A：第0行作为表头
        try:
            df_a = pd.read_excel(file_path, header=0)
            print(f"   A. header=0: {df_a.shape}, 列名: {list(df_a.columns)[:5]}")
        except Exception as e:
            print(f"   A. header=0 失败: {e}")

        # 方式B：第0行和第1行作为多级表头
        try:
            df_b = pd.read_excel(file_path, header=[0, 1])
            print(f"   B. header=[0,1]: {df_b.shape}")
            print(f"      多级列名: {df_b.columns.tolist()[:5]}")
        except Exception as e:
            print(f"   B. header=[0,1] 失败: {e}")

        # 方式C：手动处理（你的文件实际格式）
        print("\n4. 手动解析（推荐方式）:")

        # 读取所有数据
        df_all = pd.read_excel(file_path, header=None)

        # 第0行是真正的列名（如"节点名称"、"标准用气量"）
        column_names = df_all.iloc[0].tolist()
        print(f"   第0行（列名）: {column_names[:10]}")

        # 第1行是时间标识
        time_labels = df_all.iloc[1].tolist()
        print(f"   第1行（时间）: {time_labels[:10]}")

        # 从第2行开始是数据
        data_start = 2
        print(f"   数据从第{data_start}行开始")

        # 构建正确的列名
        final_columns = []
        for i, (col_name, time_label) in enumerate(zip(column_names, time_labels)):
            if pd.isna(col_name):
                col_name = f"Unnamed_{i}"

            if pd.notna(time_label):
                # 处理时间标签（可能是浮点数）
                if isinstance(time_label, float):
                    time_str = str(int(time_label)) if time_label.is_integer() else str(time_label)
                else:
                    time_str = str(time_label)

                # 清理时间字符串
                time_str = str(time_str).replace('.0', '')
                final_columns.append(f"{col_name}_{time_str}")
            else:
                final_columns.append(str(col_name))

        print(f"   最终列名: {final_columns[:10]}")

        # 提取数据
        data_df = df_all.iloc[data_start:].reset_index(drop=True)
        data_df.columns = final_columns

        print(f"   数据形状: {data_df.shape}")
        print(f"   前3行数据:")
        for i in range(min(3, len(data_df))):
            node_name = data_df.iloc[i][final_columns[0]] if len(final_columns) > 0 else "未知"
            print(f"     行{i}: 节点='{node_name}'")

        return data_df

    except Exception as e:
        print(f"❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_with_correct_parsing():
    """使用正确解析方式测试"""

    # 你的Excel文件
    excel_file = Path("tests/test_import_export/test_data/2_10_1.xlsx")

    if not excel_file.exists():
        print(f"❌ 文件不存在: {excel_file}")
        return False

    print(f"📄 测试文件: {excel_file}")

    # 1. 分析Excel结构
    data_df = analyze_excel_structure(excel_file)

    if data_df is None:
        return False

    # 2. 测试导入模块
    print("\n" + "=" * 80)
    print("测试导入模块...")

    try:
        from temporal_tree.data.storage import MemoryStore
        from temporal_tree.services.import_export.excel_importer import GasExcelImporter

        print("✅ 模块导入成功")

        # 创建导入器
        storage = MemoryStore()
        importer = GasExcelImporter(storage, {'use_midday': True})

        print("✅ 导入器创建成功")

        # 测试解析（需要先修改导入器，这里先模拟）
        print("\n模拟解析结果:")

        # 手动解析数据
        parsed_nodes = []

        # 找到节点名称列
        node_column = None
        for col in data_df.columns:
            if '节点名称' in col:
                node_column = col
                break

        if node_column is None and len(data_df.columns) > 0:
            node_column = data_df.columns[0]

        if node_column:
            for idx, row in data_df.iterrows():
                node_name = str(row[node_column]) if pd.notna(row[node_column]) else ''

                if not node_name.strip():
                    continue

                # 解析层级
                level = 0
                if node_name.startswith('  '):
                    level = 1
                elif node_name.startswith('    '):
                    level = 2

                clean_name = node_name.strip()

                # 提取时间数据
                time_data = {}

                for col in data_df.columns:
                    if col == node_column:
                        continue

                    value = row[col]
                    if pd.isna(value):
                        continue

                    # 从列名中提取时间和维度
                    col_str = str(col)

                    # 查找时间（6位数字）
                    time_match = re.search(r'(\d{6})', col_str)
                    if not time_match:
                        continue

                    time_key = time_match.group(1)

                    # 确定维度类型
                    if '标准用气量' in col_str:
                        dimension = 'standard_flow'
                    elif '表计用气量' in col_str:
                        dimension = 'metered_flow'
                    elif '标准输差量' in col_str:
                        dimension = 'standard_loss'
                    elif '表计输差量' in col_str:
                        dimension = 'metered_loss'
                    elif '标准输差率' in col_str:
                        dimension = 'standard_loss_rate'
                    elif '表计输差率' in col_str:
                        dimension = 'metered_loss_rate'
                    else:
                        continue

                    # 解析时间
                    try:
                        timestamp = parse_time_string(time_key, use_midday=True)
                        date_key = timestamp.date().isoformat()

                        if date_key not in time_data:
                            time_data[date_key] = {}

                        # 转换值
                        try:
                            if isinstance(value, str) and '%' in value:
                                # 百分比处理
                                num_value = float(value.replace('%', '')) / 100
                            else:
                                num_value = float(value)

                            time_data[date_key][dimension] = num_value
                        except:
                            continue

                    except:
                        continue

                parsed_nodes.append({
                    'row_index': idx,
                    'raw_name': node_name,
                    'node_name': clean_name,
                    'clean_name': clean_name,
                    'level': level,
                    'parent_name': None,  # 需要根据层级计算
                    'time_data': time_data,
                    'has_data': bool(time_data)
                })

            print(f"📋 解析到 {len(parsed_nodes)} 个节点")

            for i, node in enumerate(parsed_nodes[:5]):
                indent = "  " * node['level']
                print(f"   {i+1}. {indent}{node['node_name']} (层级: {node['level']})")

                if node['time_data']:
                    dates = list(node['time_data'].keys())
                    print(f"       时间点: {dates[0]} 等 {len(dates)} 个")

        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def parse_time_string(time_str, use_midday=True):
    """解析时间字符串"""
    clean_str = ''.join(c for c in str(time_str) if c.isdigit())

    if len(clean_str) == 6:
        year = int(clean_str[:4])
        month = int(clean_str[4:6])
        day = 15 if use_midday else 1

        from datetime import datetime
        return datetime(year, month, day)

    raise ValueError(f"无法解析的时间格式: {time_str}")

if __name__ == "__main__":
    success = test_with_correct_parsing()

    print("\n" + "=" * 80)
    print(f"测试结果: {'✅ 成功' if success else '❌ 失败'}")
    print("=" * 80)

    sys.exit(0 if success else 1)