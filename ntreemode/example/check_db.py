"""
查看 SQLite 数据库内容 - 纯Python方式，不需要sqlite3命令行
"""

import sqlite3
from pathlib import Path
import json
import sys

# 数据库文件路径
db_path = Path(__file__).parent / "gas_system.db"

# 检查文件是否存在
if not db_path.exists():
    print(f"❌ 数据库文件不存在: {db_path}")
    print("请先运行 complete_workflow.py 生成数据")
    sys.exit(1)

print("=" * 60)
print(f"📁 数据库文件: {db_path}")
print(f"📊 文件大小: {db_path.stat().st_size / 1024:.2f} KB")
print("=" * 60)

# 连接数据库
conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# 1. 查看所有表
print("\n📋 数据库表结构:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table['name']};")
    count = cursor.fetchone()[0]
    print(f"   📌 {table['name']}: {count} 条记录")

# 2. 查看 trees 表
print("\n🌳 树信息:")
try:
    cursor.execute("SELECT * FROM trees;")
    rows = cursor.fetchall()
    for row in rows:
        tree_data = json.loads(row['tree_data']) if row['tree_data'] else {}
        print(f"   📍 ID: {row['tree_id']}")
        print(f"      名称: {tree_data.get('name', 'N/A')}")
        print(f"      创建时间: {row.get('created_at', 'N/A')}")
except:
    print("   (无数据或表不存在)")

# 3. 查看 nodes 表
print("\n📦 节点信息:")
try:
    cursor.execute("SELECT node_id, tree_id, node_data FROM nodes LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        node_data = json.loads(row['node_data']) if row['node_data'] else {}
        print(f"   📍 ID: {row['node_id']}")
        print(f"      名称: {node_data.get('name', 'N/A')}")
        print(f"      IP: {node_data.get('ip', 'N/A')}")
        print(f"      层级: {node_data.get('level', 'N/A')}")
except:
    print("   (无数据或表不存在)")

# 4. 查看 time_series 表
print("\n⏱️  时间序列数据:")
try:
    cursor.execute("""
        SELECT node_id, dimension, timestamp, value 
        FROM time_series 
        LIMIT 10;
    """)
    rows = cursor.fetchall()
    for row in rows:
        try:
            value = json.loads(row['value']) if row['value'] else row['value']
        except:
            value = row['value']
        print(f"   📊 {row['node_id'][:8]}... {row['dimension']}: {value}")
        print(f"      🕐 {row['timestamp']}")
except:
    print("   (无数据或表不存在)")

# 5. 维度统计
print("\n📊 各维度数据量:")
try:
    cursor.execute("""
        SELECT dimension, COUNT(*) as count 
        FROM time_series 
        GROUP BY dimension 
        ORDER BY count DESC;
    """)
    rows = cursor.fetchall()
    for row in rows:
        print(f"   • {row['dimension']}: {row['count']} 条记录")
except:
    print("   (无数据或表不存在)")

# 6. 节点数据量排行
print("\n🔥 数据量最多的节点:")
try:
    cursor.execute("""
        SELECT node_id, COUNT(*) as count 
        FROM time_series 
        GROUP BY node_id 
        ORDER BY count DESC 
        LIMIT 5;
    """)
    rows = cursor.fetchall()
    for row in rows:
        # 查询节点名称
        cursor.execute("SELECT node_data FROM nodes WHERE node_id = ?", (row['node_id'],))
        node_row = cursor.fetchone()
        node_name = "未知"
        if node_row:
            try:
                node_data = json.loads(node_row['node_data'])
                node_name = node_data.get('name', '未知')
            except:
                pass
        print(f"   • {node_name}: {row['count']} 条记录")
except:
    print("   (无数据或表不存在)")

# 7. 时间范围
print("\n📅 数据时间范围:")
try:
    cursor.execute("""
        SELECT MIN(timestamp) as min_time, MAX(timestamp) as max_time 
        FROM time_series;
    """)
    row = cursor.fetchone()
    if row and row['min_time']:
        print(f"   • 最早: {row['min_time']}")
        print(f"   • 最晚: {row['max_time']}")
    else:
        print("   (无数据)")
except:
    print("   (无数据)")

conn.close()

print("\n" + "=" * 60)
print("✅ 查询完成！")
print("=" * 60)
