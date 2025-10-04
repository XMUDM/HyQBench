import json
import pandas as pd
import os
import sys
from scripts.dfs import generator
from scripts.processdb import parse_db, sample_items, sample_tables
from scripts.classify import query
import chardet

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import lotus
from lotus.models import LM, SentenceTransformersRM
lm = LM(model="gpt-4o-mini")
rm = SentenceTransformersRM(model="intfloat/e5-base-v2", device="cuda:1")
lotus.settings.configure(lm=lm, rm=rm)

def run_bird():
    # load all databases info
    with open('./data/BIRD/dev_tables.json', 'r', encoding='utf-8') as f:
        dbs = json.load(f)

    # traverse each database
    for i in range(len(dbs)):
        # parse database schema
        parse_result = parse_db(dbs[i])
        db_name = dbs[i]['db_id']
        table_names = dbs[i]['table_names_original']
        table_data = {}
        table_description = {}
        # load all tables and descriptions
        for name in table_names:
            print(dbs[i]['db_id'], name)
            path = f'./data/BIRD/{db_name}/{name}.csv'
            table_df = pd.read_csv(path, low_memory=False)
            table_data[name] = table_df
            des_path = f'./data/BIRD/description/{db_name}/database_description/{name}.csv'
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
        # sample tables
        sampled_tables_result = sample_tables(parse_result, 2)
        # sample items for each sampled table set
        for j in range(len(sampled_tables_result)):
            sampled_items_result = sample_items(sampled_tables_result[j], parse_result, table_names, table_data, 100)
            # generate data
            for query_type in query:
                for k in range(len(query[query_type])):
                    try:
                        print(f'Generating {query_type} subtype_{k} for {db_name} table {j}')
                        result_path = f'./result/flat/raw/{query_type}/subtype_{k}/{db_name}/{j}'
                        table_path = f'./result/flat/raw_table/{query_type}/subtype_{k}/{db_name}/{j}'
                        tables_data = {}
                        # copy sampled tables data to tables_data to avoid modifying the original data
                        for table_name in sampled_items_result:
                            tables_data[table_name] = sampled_items_result[table_name].copy()
                        g = generator(query[query_type][k], tables_data, table_description, result_path, table_path, query_type)
                        g.dfs_execute()
                        print(f'Finished generating {query_type} subtype_{k} for {db_name} table {j}')
                    except Exception as e:
                        import traceback
                        traceback.print_exc()

def run_text():
    tables = ['microsoft_news_500.csv', 'news_category_500.csv', 'arxiv_500.csv', 'amazon_review_500.csv']
    # load table description
    with open('./data/text/table_description.json', 'r', encoding='utf-8') as f:
        table_description = json.load(f)
    for table in tables:
        table_name = table.split('_500')[0]
        # generate data
        for query_type in query:
            for k in range(len(query[query_type])):
                try:
                    print(f'Generating {query_type} subtype_{k} for table {table_name}')
                    result_path = f'./result/flat/raw/{query_type}/subtype_{k}/{table_name}/0'
                    table_path = f'./result/flat/raw_table/{query_type}/subtype_{k}/{table_name}/0'
                    tables_data = {}
                    tables_data[table_name] = pd.read_csv(f'./data/text/{table}', low_memory=False)
                    g = generator(query[query_type][k], tables_data, table_description, result_path, table_path, query_type)
                    g.dfs_execute()
                    print(f'Finished generating {query_type} subtype_{k} for table {table_name}')
                except Exception as e:
                    import traceback
                    traceback.print_exc()

if __name__ == "__main__":
    run_bird()
    run_text()

