import json

# parse the params to get the input table names for each operator
def parse_param(process):
    inputs = []
    for i in range(len(process[0]['initial_table'])):
        inputs.append([])
    for i in range(1, len(process)):
        operator = process[i]['operator']
        param = process[i]['params']
        op_with_one_param = ['agg', 'head', 'map', 'sem_map', 'groupby', 'sem_cluster_by', 'sem_agg', 'sem_filter', 'sem_topk', 'column_calculate', 'orderby']

        param_list = []
        if operator in op_with_one_param:
            param_list.append(param.split(';;')[0])
        else:
            if operator == 'filter':
                param_list.append(param.split(';;')[0])
                op_list = param.split(';;')[2].replace('[', '').replace(']', '').replace("(", "").replace(')', '')
                op_list = [i.strip() for i in op_list.split(',')]
                for i in range(0, len(op_list), 2):
                    param_list.append(op_list[i+1])
            elif 'join' in operator:
                param_list.append(param.split(';;')[0])
                param_list.append(param.split(';;')[1])
            else:
                param_list = param.split(';;')[1:]

        inputs.append(param_list)
    return inputs

# parse the result to get the output table name for each operator
def parse_result(process):
    initial_table = process[0]['initial_table']
    results = [list(initial_table[i].keys())[0] for i in range(len(initial_table))]
    for i in range(1, len(process)):
        result = process[i]['result']
        current_table = process[i]['current_table']
        
        if 'table_length' in result:
            result = current_table[-1]
        else:
            result = result.split('result: ')[1]

        results.append(result)
    
    return results

# prune the process to remove the unnecessary operators and initial tables
def prune(process):
    initial_table = process[0]['initial_table']
    initial_table_name = [list(initial_table[i].keys())[0] for i in range(len(initial_table))]
    inputs = parse_param(process)
    results = parse_result(process)
    
    pruned_process = [process[-1]]
    flag = [False for _ in range(len(process) - 1 + len(initial_table_name))]
    flag[-1] = True
    filtered_initial_tables = []

    for i in range(len(flag) - 2, -1, -1):
        for j in range(i + 1, len(flag)):
            if not flag[j]:
                continue
            for input in inputs[j]:
                if input == results[i]:
                    flag[i] = True
                    break
            if flag[i]:
                break
                
        if i >= len(initial_table_name) and flag[i]:
            pruned_process.append(process[i + 1 - len(initial_table_name)])
        elif i < len(initial_table_name) and flag[i]:
            filtered_initial_tables.append(results[i])

    new_initial_table = []
    for i in range(len(process[0]['initial_table'])):
        if list(process[0]['initial_table'][i].keys())[0] in filtered_initial_tables:
            new_initial_table.append(process[0]['initial_table'][i])
    pruned_process.append({'initial_table': new_initial_table})
    pruned_process.reverse()

    # update the current_table for each operator
    current_table = [list(pruned_process[0]['initial_table'][i].keys())[0] for i in range(len(pruned_process[0]['initial_table']))]
    for i in range(1, len(pruned_process)):
        if 'table_length' in pruned_process[i]['result']:
            current_table.append(pruned_process[i]['result'].split(':')[0])
        pruned_process[i]['current_table'] = current_table.copy()
            
    return pruned_process
            