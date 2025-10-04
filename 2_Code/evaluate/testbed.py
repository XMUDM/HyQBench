import os
import json
import pandas as pd
import argparse
import chardet
from tqdm import tqdm
import random
import sys
sys.path.append('../')
from utils.solve import solver
from utils.blend import blend
from utils.llm import Qwen3, DP, GPT, Gemini, QwenApi
from dataGeneration.scripts.table_processing import get_tables_info
import lotus
from lotus.models import LM, SentenceTransformersRM

lm = LM(model="gpt-4o-mini")
rm = SentenceTransformersRM(model="intfloat/e5-base-v2")
lotus.settings.configure(lm=lm, rm=rm)

def main():
    # parse command line arguments
    parser = argparse.ArgumentParser(description='Testbed')
    parser.add_argument('--target', type=str, default='lotus',
                        help='Choose target')
    parser.add_argument('--model', type=str, default='DeepSeek-V3',
                        help='Choose model')
    args = parser.parse_args()

    test_data = []
    if target == 'lotus' or target == 'blendsql':
        for root, dirs, files in os.walk(f'./test/end2end'):
            for file in files:
                if file.endswith('.json'):
                    test_data.append(os.path.join(root, file))
    elif target == 'cost':
        for i in range(200):
            test_data.append(f'./test/cost/query/{i + 1}.json')

    # Instantiate model
    if 'Qwen3' in args.model:
        model = Qwen3(args.model)
    elif 'DeepSeek-V3' in args.model:
        model = DP()
    elif 'GPT-4.1' in args.model:
        model = GPT(model="gpt-4.1")
    elif 'Gemini-2.5-Flash' in args.model:
        model = Gemini()
    elif 'Qwen-Max' in args.model:
        model = QwenApi()

    for i in tqdm(range(len(test_data))):
        file_path = test_data[i]
        print(f'Processing {file_path}...')
        dir = args.model
        target = args.target
        result_path = f'./result/{target}/{dir}/answer/{i + 1}.json'
        
        if os.path.exists(result_path):
            print(f'Already processed {file_path}, skipping...')
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        db_name = data['database']
        query = data['query']

        initial_table = data['process'][0]['initial_table']
        initial_table_name = [list(initial_table[j].keys())[0] for j in range(len(initial_table))]
        
        table_data = {}
        table_description = {}
        # load table data and description
        for name in initial_table_name:
            table_data[name] = pd.read_csv(f'../dataGeneration/data/sampled_table_1k/{db_name}/{name}.csv')
            table_name = name
            if db_name == 'arxiv' or db_name == 'amazon_review' or db_name == 'news_category' or db_name == 'microsoft_news':
                with open('../dataGeneration/data/text/table_description.json', 'r', encoding='utf-8') as f:
                    table_description = json.load(f)
            else:
                try:
                    des_path = f'../dataGeneration/data/BIRD/description/{db_name}/database_description/{table_name}.csv'
                    with open(des_path, 'rb') as rawdata:
                        raw_data = rawdata.read()
                except FileNotFoundError:
                    table_name = '_'.join([word.capitalize() for word in table_name.split('_')])
                    des_path = f'../dataGeneration/data/BIRD/description/{db_name}/database_description/{table_name}.csv'
                    with open(des_path, 'rb') as rawdata:
                        raw_data = rawdata.read()
                des_df = pd.read_csv(des_path, low_memory=False, encoding=chardet.detect(raw_data)['encoding'])
                temp_des = {}
                for index, row in des_df.iterrows():
                    row['original_column_name'] = row['original_column_name'].strip()
                    row['column_description'] = row['column_description'].replace('\n', ' ') if pd.notna(row['column_description']) else 'NULL'
                    row['value_description'] = row['value_description'].replace('\n', ' ') if pd.notna(row['value_description']) else 'NULL'
                    temp_des[row['original_column_name']] = f"column_description={row['column_description']}; value_description={row['value_description']}"
                table_description[name] = temp_des

        tables = get_tables_info(table_data, table_description)

        if target == 'lotus':
            sol = solver(model, query, tables, table_data, table_description, steps=None)

            try:
                history, result, table_data = sol.solve()
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                continue
            
            # construct result dictionary
            result_dict = {
                'file_path': file_path,
                'query': query,
                'history': history,
                'result': result
            }
            

        elif target == 'blendsql':
            try:
                sql, result = blend(model, query, table_data, tables)
            except Exception as e:
                print(f'Error processing {file_path}: {e}')
                continue
            
            result_dict = {
                'file_path': file_path,
                'query': query,
                'SQL': sql,
            }

        elif target == 'cost':
            process = data['process']
            steps = []
            for j in range(1, len(process) - 1):
                if process[j]['operator'] in ['filter', 'agg', 'head', 'groupby', 'orderby', 'map', 'column_calculate', 'sem_filter', 'sem_map', 'sem_agg', 'sem_topk', 'sem_cluster_by']:
                    key_params = process[j]['params'].split(';;')[1:]
                elif process[j]['operator'] in ['join', 'sem_join']:
                    key_params = process[j]['params'].split(';;')[2:]
                else:
                    key_params = process[j]['params'].split(';;')
                steps.append({
                    'action': process[j]['operator'],
                    'key_params_without_table_operated_on': key_params
                })

            random.seed(42)
            random.shuffle(steps)

            sol = solver(model, query, tables, table_data, table_description, steps=steps)

            try:
                history, result, table_data = sol.solve()
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                continue
            
            result_dict = {
                'file_path': file_path,
                'query': query,
                'history': history,
                'result': result
            }
            
        if not os.path.exists(f'./result/{target}/{dir}/answer'):
            os.makedirs(f'./result/{target}/{dir}/answer')
        # save result to json file
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=4)
        
        # save tables

        if target == 'lotus' or target == 'cost':
            table_dir = f'./result/{target}/{dir}/table/{i + 1}'
            if not os.path.exists(table_dir):
                os.makedirs(table_dir)
            for table_name in table_data.keys():
                table_data[table_name].to_csv(f'{table_dir}/{table_name}.csv', index=False)

        elif target == 'blendsql':
            table_dir = f'./result/{target}/{dir}/table'
            if not os.path.exists(table_dir):
                os.makedirs(table_dir)
            result.df.to_csv(os.path.join(table_dir, f'{i + 1}.csv'), index=False)

if __name__ == "__main__":
    main()