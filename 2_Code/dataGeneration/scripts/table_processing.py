import pandas as pd

# get all tables info
def get_tables_info(tables, description={}):
    tables_info = []
    for name, data in tables.items():
        if description == {}:
            tables_info.append({name: get_table_info(data)})
        else:
            tables_info.append({name: get_table_info(data, description[name])})
    return tables_info

# get single table info
def get_table_info(table, description={}):
    table_info = {'*': 'all columns'}
    if description != {}:
        for column in table:
            if table[column].dtype == 'object' and pd.notna(table.iloc[0][column]) and len(table.iloc[0][column]) > 50:
                table_info[column] = 'description: ' + description[column] + ' dtype=' + str(table[column].dtype) + ' e.g. ' + table.iloc[0][column]
            else:
                if len(table[column].drop_duplicates()) <= 6:
                    table_info[column] = 'description: ' + description[column] + ' dtype=' + str(table[column].dtype) + ' e.g. ' + '; '.join([str(x) for x in table[column].drop_duplicates().tolist()])
                else:
                    table_info[column] = 'description: ' + description[column] + ' dtype=' + str(table[column].dtype) + ' e.g. ' + '; '.join([str(x) for x in table[column].drop_duplicates().sample(6).tolist()])
    else:
        for column in table:
            if table[column].dtype == 'object' and pd.notna(table.iloc[0][column]) and len(table.iloc[0][column]) > 50:
                table_info[column] = 'dtype=' + str(table[column].dtype) + ' e.g. ' + table.iloc[0][column]
            else:
                if len(table[column].drop_duplicates()) <= 6:
                    table_info[column] = 'dtype=' + str(table[column].dtype) + ' e.g. ' + '; '.join([str(x) for x in table[column].drop_duplicates().tolist()])
                else:
                    table_info[column] = 'dtype=' + str(table[column].dtype) + ' e.g. ' + '; '.join([str(x) for x in table[column].drop_duplicates().sample(6).tolist()])
    
    return table_info

# apply operator on tables
def apply_operator(operator, params, tables_data):
    from .tool import callTool
    temp_data = tables_data.copy()
    operator = operator.translate(str.maketrans('', '', '"\'')).strip()
    params = params.translate(str.maketrans('', '', '"\'')).strip()
    params = [arg.strip() for arg in params.split(";;")]
    if operator == 'value_calculate':
        table_name = '*'
    else:
        table_name = params[0]
        params = params[1:]

    res = callTool(temp_data, table_name, operator, params)
    if isinstance(res, pd.DataFrame):
        if res.empty:
            res = 'result: Empty table'
        else:
            if f"{table_name}_{operator}" in temp_data:
                for i in range(2, 10):
                    if f"{table_name}_{operator}_{i}" not in temp_data:
                        temp_name = f"{table_name}_{operator}_{i}"
                        temp_data[temp_name] = res
                        break
            else:
                temp_name = f"{table_name}_{operator}"
                temp_data[temp_name] = res

            temp_table_info = get_table_info(res, {})
            res = f'{temp_name}: ' + str(temp_table_info) + ' table_length: ' + str(res.shape[0])

    else:
        res = f"result: {res}"

    return res, temp_data

