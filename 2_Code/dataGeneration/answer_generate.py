import os
import sys
import pandas as pd
import json
from time import time
from tqdm import tqdm
from scripts.table_processing import apply_operator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import lotus
from lotus.models import LM, SentenceTransformersRM
lm = LM(model="gpt-4o-mini")
rm = SentenceTransformersRM(model="intfloat/e5-base-v2", device="cuda:1")
lotus.settings.configure(lm=lm, rm=rm)

process_path = './result'

def rep(params, history):
    for i in range(len(history)):
        if history[i]['operator'] == 'agg' or history[i]['operator'] == 'value_calculate':
            return params.replace('<placeholder>', history[i]['result'])

total_files = []

for root, dirs, files in os.walk(process_path):
    if 'optimized' not in root:
        continue
    for file in files:
        file_path = os.path.join(root, file)
        if 'flat' in file_path:
            db_name = root.split('/')[-2]
        else:
            db_name = root.split('/')[-3]
        total_files.append((file_path, db_name))

for i in tqdm(range(len(total_files))):
    file_path, db_name = total_files[i]

    # load process
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Processing {file_path}...")
    
    initial_table = data[0]['initial_table']
    initial_table_name = [list(initial_table[j].keys())[0] for j in range(len(initial_table))]
    
    # load initial tables
    table_data = {}
    for name in initial_table_name:
        table_data[name] = pd.read_csv(f'./data/sampled_table_1k/{db_name}/{name}.csv')

    history = []
    for j in range(1, len(data)):
        operator = data[j]['operator']
        params = data[j]['params']
        if '<placeholder>' in params:
            params = rep(params, history)
        start_time = time()
        res, table_data = apply_operator(operator, params, table_data)
        end_time = time()
        flag = True
        if res == 'result: Empty table' or 'Invalid' in res:
            flag = False
            print(f"Error in {file_path}: {res}")
            break
        history.append({
            'operator': operator,
            'params': params,
            'result': res,
            'current_table': [name for name in table_data.keys()],
            'execution_time': end_time - start_time
        })

    if flag:
        # save result
        output_path = file_path.replace('optimized', 'answer')
        if not os.path.exists(output_path):
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=4)
                  
        table_dir = output_path.replace('answer', 'answer_table').split('.json')[0]
        if not os.path.exists(table_dir):
            os.makedirs(table_dir)
        for name in table_data.keys():
            table_data[name].to_csv(os.path.join(table_dir, f'{name}.csv'), index=False)
        print(f"Processed {file_path} successfully.")
    else:
        print(f"Failed to process {file_path}")
