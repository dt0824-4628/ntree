"""
直接调用示例：Excel → 时间树
"""
import sys
from pathlib import Path

# 添加src到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from temporal_tree.data.storage import MemoryStore, JSONStore
from temporal_tree.services.import_export.excel_importer import GasExcelImporter


def import_excel_to_tree(excel_file: str, output_format: str = 'memory'):
    """
    导入Excel文件并创建时间树

    Args:
        excel_file: Excel文件路径
        output_format: 输出格式 ('memory', 'json', 'sqlite')

    Returns:
        导入结果
    """
    print("=" * 80)
    print("Excel → 时间树导入工具")
    print("=" * 80)

    # 1. 创建存储
    if output_format == 'json':
        storage = JSONStore('output/gas_tree.json')
        print("✅ 使用JSON存储")
    elif output_format == 'sqlite':
        from temporal_tree.data.storage import SQLiteStore
        storage = SQLiteStore('output/gas_tree.db')
        print("✅ 使用SQLite存储")
    else:
        storage = MemoryStore()
        print("✅ 使用内存存储")

    # 2. 创建导入器
    importer = GasExcelImporter(
        storage=storage,
        config={
            'use_midday': True,
            'auto_calculate_loss': True
        }
    )

    print(f"📄 导入文件: {excel_file}")

    try:
        # 3. 执行完整导入
        result = importer.import_and_create_tree(
            file_path=excel_file,
            tree_name=f"燃气系统_{Path(excel_file).stem}"
        )

        # 4. 显示结果
        print("\n✅ 导入成功！")
        print(f"🌳 树ID: {result['tree_id']}")
        print(f"📊 统计:")
        print(f"   节点数: {len(result['nodes'])}")
        print(f"   时间点: {len(result['time_points'])}")
        print(f"   层级深度: {max([n['level'] for n in result['nodes']]) + 1}")

        # 5. 显示前几个节点
        print(f"\n🌿 节点示例:")
        for i, node in enumerate(result['nodes'][:10]):
            indent = "  " * node['level']
            parent = f" (父: {node['parent_id']})" if node['parent_id'] else ""
            print(f"   {i + 1:2}. {indent}{node['name']}{parent}")

        if len(result['nodes']) > 10:
            print(f"   ... 还有 {len(result['nodes']) - 10} 个节点")

        # 6. 显示维度统计
        dimensions = set()
        for tp in result['time_points']:
            dimensions.add(tp['dimension'])

        print(f"\n📈 数据维度: {', '.join(sorted(dimensions))}")

        # 7. 保存结果（可选）
        if output_format == 'memory':
            print(f"\n💾 数据保存在内存中")
            print(f"   使用 storage.load_tree('{result['tree_id']}') 访问")
        else:
            print(f"\n💾 数据已保存到文件")

        return result

    except Exception as e:
        print(f"\n❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='导入Excel文件到时间树系统')
    parser.add_argument('excel_file', help='Excel文件路径')
    parser.add_argument('--output', choices=['memory', 'json', 'sqlite'],
                        default='memory', help='输出格式')
    parser.add_argument('--tree-name', help='自定义树名称')

    args = parser.parse_args()

    # 执行导入
    result = import_excel_to_tree(args.excel_file, args.output)

    if result:
        print("\n" + "=" * 80)
        print("🎉 导入完成！")
        print("=" * 80)

        # 可以进一步处理结果
        # 例如：保存为JSON文件
        if args.output == 'memory':
            import json
            from datetime import date

            output_file = Path(args.excel_file).with_suffix('.json')

            def convert_for_json(obj):
                if isinstance(obj, (date, datetime)):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=convert_for_json)

            print(f"📁 结果已保存到: {output_file}")

    return result is not None


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)