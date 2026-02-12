"""
燃气Excel导入器 - 完整整合版
可以直接生成时间树
"""
import os
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from .base_importer import DataImporter, ImportError


class GasExcelImporter(DataImporter):
    """
    燃气Excel导入器 - 可以直接生成时间树

    功能：
    1. 解析特殊格式Excel（第0行列名，第1行时间）
    2. 构建完整的节点层级关系
    3. 生成可直接导入时间树系统的数据
    """

    def __init__(self, storage, config: Dict = None):
        super().__init__(config)
        self.storage = storage
        self.config = config or {}
        self.use_midday = self.config.get('use_midday', True)

        # 统计信息
        self.stats = {
            'files_processed': 0,
            'nodes_parsed': 0,
            'time_points': 0,
            'dimensions_added': 0,
            'trees_created': 0
        }

    # ============ 抽象方法实现 ============

    def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """提取文件元数据"""
        metadata = {
            'file_path': file_path,
            'file_name': Path(file_path).name,
            'import_time': datetime.now().isoformat(),
            'config': self.config
        }

        try:
            if os.path.exists(file_path):
                file_stat = os.stat(file_path)
                metadata.update({
                    'file_size': file_stat.st_size,
                    'modified_time': datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                })
        except:
            pass

        return metadata

    def parse_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        解析Excel数据 - 主方法

        支持格式：第0行列名，第1行时间，第2行开始数据
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("需要pandas库，请运行: pip install pandas openpyxl")

        if not self.validate_file(file_path):
            raise ImportError(f"无效的文件: {file_path}")

        try:
            # 读取原始数据
            df_raw = pd.read_excel(file_path, header=None)

            if len(df_raw) < 3:
                raise ImportError("Excel文件数据太少，至少需要3行")

            # 获取列名和时间标签
            column_names = df_raw.iloc[0].tolist()
            time_labels = df_raw.iloc[1].tolist()

            # 构建最终列名
            final_columns = []
            for i, (col_name, time_label) in enumerate(zip(column_names, time_labels)):
                if pd.isna(col_name):
                    col_name = f"Column_{i}"
                else:
                    col_name = str(col_name)

                if pd.notna(time_label):
                    # 处理时间标签
                    if isinstance(time_label, float):
                        time_str = str(int(time_label)) if time_label.is_integer() else str(time_label)
                    else:
                        time_str = str(time_label)

                    # 清理
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
            current_hierarchy = []  # 存储(level, name)元组

            # 找到节点名称列
            node_column = self._find_node_column(final_columns)

            if node_column is None:
                raise ImportError("未找到节点名称列")

            for idx, row in data_df.iterrows():
                raw_name = str(row[node_column]) if pd.notna(row[node_column]) else ''

                if not raw_name.strip():
                    continue

                # 解析层级
                level = self._parse_level(raw_name)
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

                    # 从列名提取时间和维度
                    col_str = str(col)
                    time_match = re.search(r'(\d{6})', col_str)
                    if not time_match:
                        continue

                    time_key = time_match.group(1)
                    dimension = self._extract_dimension(col_str)

                    if not dimension:
                        continue

                    # 解析时间
                    try:
                        timestamp = self._parse_time_string(time_key)
                        date_key = timestamp.date().isoformat()

                        if date_key not in time_data:
                            time_data[date_key] = {}

                        # 转换值
                        try:
                            if isinstance(value, str) and '%' in value:
                                num_value = float(value.replace('%', '')) / 100
                            else:
                                num_value = float(value)

                            time_data[date_key][dimension] = num_value
                        except (ValueError, TypeError):
                            continue

                    except (ValueError, TypeError):
                        continue

                # 构建节点数据
                node_data = {
                    'row_index': idx,
                    'raw_name': raw_name,
                    'node_name': clean_name,
                    'clean_name': clean_name,
                    'level': level,
                    'parent_name': parent_name,
                    'time_data': time_data,
                    'has_data': bool(time_data)
                }

                parsed_nodes.append(node_data)
                self.stats['nodes_parsed'] += 1

            self.stats['files_processed'] += 1
            return parsed_nodes

        except Exception as e:
            raise ImportError(f"解析Excel失败: {str(e)}")

    def convert_to_tree_nodes(self, parsed_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        转换为可直接导入时间树系统的格式

        返回格式：
        {
            'tree_id': '生成的树ID',
            'tree_data': {...},      # 树基本信息
            'nodes': [...],          # 所有节点
            'time_points': [...],    # 时间点数据
            'statistics': {...}      # 统计信息
        }
        """
        try:
            # 生成树ID
            tree_id = f"gas_tree_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # 树基本信息
            tree_data = {
                'tree_id': tree_id,
                'name': '燃气输差分析树',
                'description': '从Excel导入的燃气输差分析数据',
                'created_at': datetime.now(),
                'source': 'excel_import',
                'node_count': len(parsed_data),
                'metadata': {
                    'import_config': self.config,
                    'import_time': datetime.now().isoformat()
                }
            }

            # 转换节点
            tree_nodes = []
            time_points_data = []

            # 建立名称到ID的映射
            name_to_id = {}

            # 第一遍：创建所有节点，建立映射
            for node_data in parsed_data:
                node_id = self._generate_node_id(node_data)
                name_to_id[node_data['clean_name']] = node_id

                # 查找父节点ID
                parent_id = None
                if node_data['parent_name'] and node_data['parent_name'] in name_to_id:
                    parent_id = name_to_id[node_data['parent_name']]

                # 生成IP地址
                ip_address = self._generate_ip_address(node_data['level'], node_id)

                # 构建节点
                node_dict = {
                    'node_id': node_id,
                    'tree_id': tree_id,
                    'name': node_data['clean_name'],
                    'original_name': node_data['raw_name'],
                    'level': node_data['level'],
                    'parent_id': parent_id,
                    'ip_address': ip_address,
                    'created_at': datetime.now(),
                    'metadata': {
                        'import_source': 'excel',
                        'row_index': node_data['row_index'],
                        'has_data': node_data['has_data'],
                        'raw_name': node_data['raw_name']
                    }
                }

                tree_nodes.append(node_dict)

            # 第二遍：提取时间点数据
            for node_data in parsed_data:
                node_id = name_to_id[node_data['clean_name']]

                for date_str, dimensions in node_data['time_data'].items():
                    timestamp = datetime.fromisoformat(date_str + 'T12:00:00')

                    for dimension, value in dimensions.items():
                        time_point = {
                            'tree_id': tree_id,
                            'node_id': node_id,
                            'dimension': dimension,
                            'value': value,
                            'timestamp': timestamp,
                            'date_str': date_str,
                            'metadata': {
                                'source': 'excel_import',
                                'node_name': node_data['clean_name']
                            }
                        }

                        time_points_data.append(time_point)
                        self.stats['time_points'] += 1
                        self.stats['dimensions_added'] += 1

            self.stats['trees_created'] += 1

            return {
                'tree_id': tree_id,
                'tree_data': tree_data,
                'nodes': tree_nodes,
                'time_points': time_points_data,
                'statistics': self.stats.copy(),
                'success': True,
                'message': f"成功导入 {len(parsed_data)} 个节点，{len(time_points_data)} 个时间点"
            }

        except Exception as e:
            raise ImportError(f"转换数据失败: {str(e)}")

    # ============ 工具方法 ============

    def validate_file(self, file_path: str) -> bool:
        """验证文件"""
        if not os.path.exists(file_path):
            return False

        ext = Path(file_path).suffix.lower()
        return ext in ['.xlsx', '.xls', '.xlsm']

    def _find_node_column(self, columns: List[str]) -> Optional[str]:
        """查找节点名称列"""
        for col in columns:
            if '节点' in col or '名称' in col or 'name' in col.lower():
                return col

        return columns[0] if columns else None

    def _parse_level(self, raw_name: str) -> int:
        """解析层级"""
        if not raw_name or raw_name.isspace():
            return 0

        leading_spaces = len(raw_name) - len(raw_name.lstrip())

        # 每2个空格算一级
        level = leading_spaces // 2

        # 如果顶级节点也有缩进，可以调整
        # level = max(level - 1, 0)  # 如果需要调整

        return min(level, 10)

    def _extract_dimension(self, column_name: str) -> Optional[str]:
        """从列名提取维度类型"""
        col_str = str(column_name).lower()

        if '标准用气量' in col_str or 'standard_flow' in col_str:
            return 'standard_flow'
        elif '表计用气量' in col_str or 'metered_flow' in col_str:
            return 'metered_flow'
        elif '标准输差量' in col_str:
            return 'standard_loss'
        elif '表计输差量' in col_str:
            return 'metered_loss'
        elif '标准输差率' in col_str:
            return 'standard_loss_rate'
        elif '表计输差率' in col_str:
            return 'metered_loss_rate'

        return None

    def _parse_time_string(self, time_str: str) -> datetime:
        """解析时间字符串"""
        # 清理
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
            day = 15 if self.use_midday else 1

            if 1 <= month <= 12:
                return datetime(year, month, day)

        raise ValueError(f"无法解析的时间格式: {time_str}")

    def _generate_node_id(self, node_data: Dict) -> str:
        """生成节点ID"""
        name_part = re.sub(r'[^\w]', '_', node_data['clean_name'].lower())
        return f"node_{name_part}_{node_data['level']}_{node_data['row_index']}"

    def _generate_ip_address(self, level: int, node_id: str) -> str:
        """生成IP地址"""
        # 使用节点ID的哈希生成唯一值
        import hashlib
        hash_val = int(hashlib.md5(node_id.encode()).hexdigest()[:8], 16)

        # 基础IP: 10.0.0.0
        base_parts = [10, 0, 0]

        # 根据层级和哈希值生成
        for i in range(min(level + 1, 4)):
            if i < 3:
                base_parts[i] = (base_parts[i] + (hash_val >> (i * 8)) % 256) % 256
            else:
                base_parts.append((hash_val >> 24) % 256)

        # 确保不超过4段
        while len(base_parts) > 4:
            base_parts.pop()

        return '.'.join(str(p) for p in base_parts)

    # ============ 高级方法 ============

    def import_and_create_tree(self, file_path: str, tree_name: str = None) -> Dict[str, Any]:
        """
        完整导入流程：Excel → 时间树

        Args:
            file_path: Excel文件路径
            tree_name: 可选的树名称

        Returns:
            包含树数据、节点、时间点的完整结果
        """
        # 1. 解析Excel
        parsed_data = self.parse_data(file_path)

        # 2. 转换为树格式
        result = self.convert_to_tree_nodes(parsed_data)

        # 3. 可选：设置自定义树名
        if tree_name:
            result['tree_data']['name'] = tree_name

        # 4. 可选：保存到存储系统
        if self.storage:
            self._save_to_storage(result)

        return result

    def _save_to_storage(self, import_result: Dict[str, Any]):
        """保存到存储系统"""
        try:
            tree_data = import_result['tree_data']
            nodes = import_result['nodes']
            time_points = import_result['time_points']

            # 保存树
            self.storage.save_tree(tree_data)

            # 保存所有节点
            for node in nodes:
                self.storage.save_node(tree_data['tree_id'], node)

            # 保存时间点数据
            for tp in time_points:
                self.storage.save_node_data(
                    tree_id=tp['tree_id'],
                    node_id=tp['node_id'],
                    dimension=tp['dimension'],
                    value=tp['value'],
                    timestamp=tp['timestamp']
                )

            import_result['storage_saved'] = True
            import_result['storage_message'] = "数据已保存到存储系统"

        except Exception as e:
            import_result['storage_saved'] = False
            import_result['storage_error'] = str(e)

    def get_import_statistics(self) -> Dict[str, Any]:
        """获取导入统计信息"""
        return self.stats.copy()

    def reset_statistics(self):
        """重置统计信息"""
        self.stats = {
            'files_processed': 0,
            'nodes_parsed': 0,
            'time_points': 0,
            'dimensions_added': 0,
            'trees_created': 0
        }


# 导出
__all__ = ['GasExcelImporter']