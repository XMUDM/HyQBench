import json
import pandas as pd
import os
import chardet
import copy
from tqdm import tqdm
import traceback
import sys
from scripts.processdb import get_valid_tables
from scripts.nested import nestedGenerator
from scripts.table_processing import get_table_info

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import lotus
from lotus.models import LM, SentenceTransformersRM
lm = LM(model="gpt-4o-mini")
rm = SentenceTransformersRM(model="intfloat/e5-base-v2", device="cuda:1")
lotus.settings.configure(lm=lm, rm=rm)

subquery = []

# traverse all json files in result folder
for root, dirs, files in os.walk('./result/flat'):
    for file in files:
        if file.endswith('.json'):
            subquery.append((root, file))

print(f'Total files to process: {len(subquery)}')

# traverse each json file
for i in tqdm(range(len(subquery))):
    root, file = subquery[i]
    with open(os.path.join(root, file), 'r') as f:
        data = json.load(f)
    print(f'file_path: {root}/{file}')
    no = file.split('.')[0]
    table_path = f'{root}/raw_table/{file.split(".")[0]}'
    print(f'table_path: {table_path}')
    if not os.path.exists(table_path):
        continue

    # load all tables
    tables_data = {}
    for table_name in data[-1]['current_table']:
        table = pd.read_csv(os.path.join(table_path, f'{table_name}.csv'))
        tables_data[table_name] = table

    try:
        db_name = root.split('/')[4]
        valid_tables = get_valid_tables(data, tables_data, db_name)
        valid_tables["NULL"] = None
    except Exception as e:
        print(f'Get valid tables for {root}/{no} Error Message: {e}')
        continue

    count = 0
    for table_name, table_data in valid_tables.items():
        try:
            count += 1
            result_dir = f'./result/nested/raw/{root.split("raw/")[1]}/{no}'
            print(f'Processing {result_dir} ({count}/{len(valid_tables)})')
            history = copy.deepcopy(data)
            temp_tables_data = copy.deepcopy(tables_data)
            if table_name != 'NULL':
                des_path = f'./data/BIRD/description/{db_name}/database_description/{table_name}.csv'
                try:
                    with open(des_path, 'rb') as rawdata:
                        raw_data = rawdata.read()
                except FileNotFoundError:
                    # Try to find the file with capitalized table name
                    temp_table_name = '_'.join([word.capitalize() for word in table_name.split('_')])
                    des_path = f'../data/BIRD/description/{db_name}/database_description/{temp_table_name}.csv'
                    with open(des_path, 'rb') as rawdata:
                        raw_data = rawdata.read()
                des_df = pd.read_csv(des_path, low_memory=False, encoding=chardet.detect(raw_data)['encoding'])
                des = {}
                for index, row in des_df.iterrows():
                    row['original_column_name'] = row['original_column_name'].strip()
                    row['column_description'] = row['column_description'].replace('\n', ' ') if pd.notna(row['column_description']) else 'NULL'
                    row['value_description'] = row['value_description'].replace('\n', ' ') if pd.notna(row['value_description']) else 'NULL'
                    des[row['original_column_name']] = f"column_description={row['column_description']}; value_description={row['value_description']}"
                history[0]['initial_table'].append({
                    table_name: get_table_info(table_data, des)
                })
                for i in range(1, len(history)):
                    history[i]['current_table'].insert(len(history[0]['initial_table']) - 1, table_name)
                temp_tables_data[table_name] = table_data
                temp_tables_data = {k: temp_tables_data[k] for k in history[-1]['current_table'] if k in temp_tables_data}

            g = nestedGenerator(temp_tables_data, history)
            total_history, total_data = g.generate()

            if type(total_history) == str:
                print(f'Failed to generate for {result_dir} ({count}/{len(valid_tables)}) Error Message: {total_history}')
                continue

            if not os.path.exists(result_dir):
                os.makedirs(result_dir)
            with open(f'{result_dir}/{count}.json', 'w') as f:
                json.dump(total_history, f, indent=4)

            print(f'Completed {result_dir} ({count}/{len(valid_tables)})')
        except Exception as e:
            traceback.print_exc()
            print(f'Error processing {result_dir} ({count}/{len(valid_tables)}) Error Message: {e}')
