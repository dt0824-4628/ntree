import os
import sys


def print_tree(directory, prefix="", depth=None, current_depth=0, show_hidden=False):
    """
    递归打印目录树结构

    Args:
        directory: 要显示的目录路径
        prefix: 当前行的前缀（用于缩进）
        depth: 最大深度，None表示不限制
        current_depth: 当前深度
        show_hidden: 是否显示隐藏文件（以.开头的文件）
    """
    # 检查深度限制
    if depth is not None and current_depth >= depth:
        return

    # 获取目录内容并排序
    try:
        items = sorted(os.listdir(directory))
    except PermissionError:
        print(f"{prefix}└── [权限拒绝]")
        return

    # 过滤出文件和目录（排除隐藏文件）
    files = []
    dirs = []

    for item in items:
        # 如果不显示隐藏文件，跳过以.开头的项
        if not show_hidden and item.startswith('.'):
            continue

        item_path = os.path.join(directory, item)
        if os.path.isdir(item_path):
            dirs.append(item)
        else:
            files.append(item)

    # 先打印目录，再打印文件
    all_items = dirs + files
    total_items = len(all_items)

    for i, item in enumerate(all_items):
        item_path = os.path.join(directory, item)
        is_last = (i == total_items - 1)

        # 当前行的连接符
        connector = "└── " if is_last else "├── "

        # 打印当前项
        print(f"{prefix}{connector}{item}")

        # 如果是目录，递归处理
        if item in dirs:
            # 计算下一级的前缀
            extension = "    " if is_last else "│   "
            print_tree(item_path, prefix + extension, depth, current_depth + 1, show_hidden)


def main():
    import argparse

    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='显示目录树结构')
    parser.add_argument('directory', nargs='?', default='.', help='要显示的目录路径（默认为当前目录）')
    parser.add_argument('-d', '--depth', type=int, help='限制显示深度')
    parser.add_argument('-a', '--all', action='store_true', help='显示所有文件，包括隐藏文件')

    args = parser.parse_args()

    # 获取要显示的目录
    root_dir = args.directory

    # 检查目录是否存在
    if not os.path.exists(root_dir):
        print(f"错误：目录 '{root_dir}' 不存在")
        return

    # 转换为绝对路径
    root_dir = os.path.abspath(root_dir)

    print(f"目录树: {root_dir}")
    print_tree(root_dir, depth=args.depth, show_hidden=args.all)


if __name__ == "__main__":
    main()