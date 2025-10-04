import os
import json
import pandas as pd
from scripts.op_tree import build_tree
from scripts.prune import prune

dir = './result'

total_files = []

for root, dirs, files in os.walk(dir):
    if 'raw' not in root:
        continue
    for file in files:
        if file.endswith('json'):
            file_path = os.path.join(root, file)
            total_files.append(file_path)

for i in range(len(total_files)):
    file_path = total_files[i]
    print(f"Processing the {i + 1}th file: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        process = json.load(f)
    process = prune(process)
    tables_data = {}
    initial_table = process[0]['initial_table']
    tables_name = [list(initial_table[j].keys())[0] for j in range(len(initial_table))]
    db_name = file_path.split('/')[6]
    for name in tables_name:
        with open(f'./data/sampled_table_1k/{db_name}/{name}.csv', 'r', encoding='utf-8') as f:
            table_df = pd.read_csv(f, low_memory=False)
            tables_data[name] = table_df
    try:
        op_tree = build_tree(process, tables_data)
    except Exception as e:
        print(e)
        continue

    result_path = file_path.replace('raw', 'optimized')
    if not os.path.exists(os.path.dirname(result_path)):
        os.makedirs(os.path.dirname(result_path))
    topo_sort_json = op_tree.tree2json()
    process = []
    for j in range(len(topo_sort_json) - 1, -1, -1):
        if topo_sort_json[j]['operator'] != 'init':
            process.append({
                'operator': topo_sort_json[j]['operator'],
                'params': topo_sort_json[j]['params'],
                'result': topo_sort_json[j]['result']
            })
    process.append({'initial_table': initial_table})
    process.reverse()
    current_table = tables_name
    for j in range(1, len(process)):
        if process[j]['operator'] != 'agg' and process[j]['operator'] != 'value_calculate':
            current_table.append(process[j]['result'])
        process[j]['current_table'] = current_table.copy()

    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(process, f, indent=4)