import json
import sys
sys.path.append('..')
from dataGeneration.scripts.op_tree import parse_cols

class preprocessor_cost:
    def __init__(self):
        pass

    def preprocess_table(self, tables, query, steps):
        name = list(tables[0].keys())[0]
        if name != 'arxiv' and name != 'amazon_review' and name != 'news_category' and name != 'microsoft_news':
            with open('../dataGeneration/data/BIRD/dev_tables.json', 'r', encoding='utf-8') as f:
                dbs = json.load(f)
                for db in dbs:
                    if name in db['table_names_original']:
                        target_db = db
                        break
                keys = {table_name: [] for table_name in db['table_names_original']}
                for i in range(len(target_db['primary_keys'])):
                    if type(target_db['primary_keys'][i]) == list:
                        for col in target_db['primary_keys'][i]:
                            keys[db['table_names_original'][target_db['column_names_original'][col][0]]].append(target_db['column_names_original'][col][1])
                    else:
                        keys[db['table_names_original'][target_db['column_names_original'][target_db['primary_keys'][i]][0]]].append(target_db['column_names_original'][target_db['primary_keys'][i]][1])

                for i in range(len(target_db['foreign_keys'])):
                    for col in target_db['foreign_keys'][i]:
                        column = target_db['column_names_original'][col]
                        table_name = target_db['table_names_original'][column[0]]
                        column_name = column[1]
                        if column_name not in keys[table_name]:
                            keys[table_name].append(column_name)
        else:
            keys = {name: []}

        result = []
        all_selection = []
        for step in steps:
            for item in step['key_params_without_table_operated_on']:
                if item not in all_selection:
                    if item.startswith('['):
                        item = [it.strip() for it in item[1:-1].split(",")]
                        for it in item:
                            if it.endswith(':left'):
                                it = it[:-5]
                            elif it.endswith(':right'):
                                it = it[:-6]
                            elif it.endswith('_x') or it.endswith('_y'):
                                it = it[:-3]
                            all_selection.append(it)
                    elif '{' in item:
                        cols = parse_cols(item)
                        for i in range(len(cols)):
                            if cols[i].endswith(':left'):
                                cols[i] = cols[i][:-5]
                            elif cols[i].endswith(':right'):
                                cols[i] = cols[i][:-6]
                            elif cols[i].endswith('_x') or cols[i].endswith('_y'):
                                cols[i] = cols[i][:-2]
                        all_selection.extend(cols)
                    else:
                        if item.endswith(':left'):
                            item = item[:-5]
                        elif item.endswith(':right'):
                            item = item[:-6]
                        elif item.endswith('_x') or item.endswith('_y'):
                            item = item[:-2]
                        all_selection.append(item)
        print(f"all_selection: {all_selection}")
        for item in tables:
            name = list(item.keys())[0]
            columns = list(item[name].keys())
            selected_columns = []
            for key in keys[name]:
                selected_columns.append(key)
            for column in columns:
                if column in all_selection and column not in selected_columns:
                    selected_columns.append(column)
            result.append({
                "table_name": name,
                "columns": selected_columns
            })

        return json.dumps(result, indent=4)
