import pandas as pd
import numpy as np
import json

np.random.seed(42)

def parse_db(db_info):
    tables = db_info['table_names_original']
    columns = db_info['column_names_original']
    foreign_keys = db_info['foreign_keys']
    graph = [[0] * len(tables) for _ in range(len(tables))]
    for pairs in foreign_keys:
        foreign_key = columns[pairs[0]]
        primary_key = columns[pairs[1]]
        foreign_key_column = foreign_key[1]
        primary_key_column = primary_key[1]
        graph[foreign_key[0]][primary_key[0]] = (foreign_key_column, primary_key_column)

    return graph

def sample_tables(graph, sample_size=2):
    sampled_tables = []
    path = []
    def dfs(graph, node):
        if node in path:
            return
        path.append(node)
        if len(path) == sample_size:
            sampled_tables.append(tuple(path))
            path.pop()
            return
        for i in range(len(graph[node])):
            if graph[node][i] != 0 and i not in path:
                dfs(graph, i)
        path.pop()    

    for i in range(len(graph)):
        dfs(graph, i)

    return sampled_tables
        
def sample_items(sampled_tables, graph, table_names, table_data, sample_size=1000):
    sampled_items = {}
    for i in range(len(sampled_tables)):
        name = table_names[sampled_tables[i]]
        df = table_data[name]
        if len(table_data[name]) < sample_size:
            sampled_items[name] = df
        else:
            limit = {}
            for j in range(i):
                if type(graph[sampled_tables[j]][sampled_tables[i]]) == tuple:
                    foreign_key = graph[sampled_tables[j]][sampled_tables[i]][0]
                    primary_key = graph[sampled_tables[j]][sampled_tables[i]][1]
                    if primary_key not in limit:
                        limit[primary_key] = set()
                    for row in sampled_items[table_names[sampled_tables[j]]][foreign_key]:
                        limit[primary_key].add(row)
            limit = {primary_key: list(limit[primary_key]) for primary_key in limit}
            limit_df = df.copy()
            discard_df = df.copy()
            for key in limit:
                discard_df = discard_df[~discard_df[key].isin(limit[key])]
            limit_df = limit_df[~limit_df.index.isin(discard_df.index)]
            if len(limit_df) < sample_size:
                left = sample_size - len(limit_df)
                discard_df = discard_df.sample(n=left, replace=False)
                limit_df = pd.concat([limit_df, discard_df], axis=0)
                
            else:
                temp_df = limit_df.copy()
                temp_df = temp_df.drop_duplicates([key for key in limit])
                if len(temp_df) < sample_size:
                    left = sample_size - len(temp_df)
                    drop_df = limit_df[~limit_df.index.isin(temp_df.index)]
                    drop_df = drop_df.sample(n=left, replace=False)
                    limit_df = pd.concat([limit_df, drop_df], axis=0)
                else:
                    limit_df = temp_df
                
            sampled_items[name] = limit_df

    return sampled_items

def sample_items_for_added(sampled_tables, graph, table_names, table_data, sample_size=1000, completed_tables=None, completed_tables_data=None):
    sampled_items = {}
    for table in completed_tables:
        sampled_items[table_names[table]] = completed_tables_data[table_names[table]]
    for i in range(len(sampled_tables)):
        name = table_names[sampled_tables[i]]
        if name in sampled_items:
            continue

        df = table_data[name]
        if len(table_data[name]) < sample_size:
            sampled_items[name] = df
        else:
            limit = {}
            for j in range(len(sampled_tables)):
                if type(graph[sampled_tables[j]][sampled_tables[i]]) == tuple:
                    foreign_key = graph[sampled_tables[j]][sampled_tables[i]][0]
                    primary_key = graph[sampled_tables[j]][sampled_tables[i]][1]
                    if primary_key not in limit:
                        limit[primary_key] = set()
                    for row in sampled_items[table_names[sampled_tables[j]]][foreign_key]:
                        limit[primary_key].add(row)
                if type(graph[sampled_tables[i]][sampled_tables[j]]) == tuple:
                    foreign_key = graph[sampled_tables[i]][sampled_tables[j]][0]
                    primary_key = graph[sampled_tables[i]][sampled_tables[j]][1]
                    if foreign_key not in limit:
                        limit[foreign_key] = set()
                    for row in sampled_items[table_names[sampled_tables[j]]][primary_key]:
                        limit[foreign_key].add(row)
            limit = {primary_key: list(limit[primary_key]) for primary_key in limit}
            limit_df = df.copy()
            discard_df = df.copy()
            for key in limit:
                discard_df = discard_df[~discard_df[key].isin(limit[key])]
            limit_df = limit_df[~limit_df.index.isin(discard_df.index)]
            if len(limit_df) < sample_size:
                left = sample_size - len(limit_df)
                discard_df = discard_df.sample(n=left, replace=False)
                limit_df = pd.concat([limit_df, discard_df], axis=0)
                
            else:
                temp_df = limit_df.copy()
                temp_df = temp_df.drop_duplicates([key for key in limit])
                if len(temp_df) < sample_size:
                    left = sample_size - len(temp_df)
                    drop_df = limit_df[~limit_df.index.isin(temp_df.index)]
                    drop_df = drop_df.sample(n=left, replace=False)
                    limit_df = pd.concat([limit_df, drop_df], axis=0)
                else:
                    limit_df = temp_df
                
            sampled_items[name] = limit_df

    return sampled_items

def get_valid_tables(history, tables_data, db_name):
    initial_table = history[0]['initial_table']
    completed_tables = [list(initial_table[i].keys())[0] for i in range(len(initial_table))]
    completed_tables_data = {}
    for table in completed_tables:
        completed_tables_data[table] = tables_data[table]

    with open('../data/BIRD/dev_tables.json', 'r', encoding='utf-8') as f:
        dbs = json.load(f)

    valid_tables = {}
    for db in dbs:
        if db['db_id'] == db_name:
            graph = parse_db(db)
            tables_name = db['table_names_original']
            sampled_tables = sample_tables(graph, sample_size=3)
            # 将表名转化为下标
            completed_tables = [tables_name.index(table) for table in completed_tables]

            db_data = {}
            for name in tables_name:
                path = f'../data/BIRD/{db_name}/{name}.csv'
                table_df = pd.read_csv(path, low_memory=False)
                db_data[name] = table_df
            
            for result in sampled_tables:
                flag = True
                for table in completed_tables:
                    if table not in result:
                        flag = False
                        break
                if flag:
                    # get tables that is not in completed_tables
                    valid_table = [table for table in result if table not in completed_tables][0]
                    sampled_items = sample_items_for_added(result, graph, tables_name, db_data, sample_size=100, completed_tables=completed_tables, completed_tables_data=completed_tables_data)
                    valid_tables[tables_name[valid_table]] = sampled_items[tables_name[valid_table]]

    return valid_tables
