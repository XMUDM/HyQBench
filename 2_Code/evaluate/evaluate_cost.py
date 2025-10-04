import os
import json
import pandas as pd
import sys
sys.path.append('..')
from dataGeneration.scripts.op_tree import build_tree

def parse_steps(process):
    steps = []
    for j in range(len(process)):
        if 'operator' in process[j]:
            operator = process[j]['operator']
        else:
            operator = process[j]['action']
        if 'params' in process[j]:
            params = process[j]['params']
        else:
            params = process[j]['action_input']
        if operator in ['filter', 'agg', 'head', 'groupby', 'orderby', 'map', 'column_calculate', 'sem_filter', 'sem_map', 'sem_agg', 'sem_topk', 'sem_cluster_by']:
            key_params = params.split(';;')[1:]
        elif operator in ['join']:
            key_params = params.split(';;')[2:]
            key_params.sort()
        elif operator in ['sem_join']:
            key_params = params.split(';;')[2:]
        else:
            key_params = params.split(';;')
        steps.append({
            'action': operator,
            'key_params_without_table_operated_on': [param.replace('"', '').replace("'", '').strip() for param in key_params]
        })

    return steps

def check(prediction, ground_truth):
    if len(prediction) != len(ground_truth):
        return False

    try:
        prediction_steps = parse_steps(prediction)
        ground_truth_steps = parse_steps(ground_truth)
    except Exception as e:
        return False

    for i in range(len(prediction_steps)):
        if prediction_steps[i] not in ground_truth_steps:
            return False
        
    for i in range(len(ground_truth_steps)):
        if ground_truth_steps[i] not in prediction_steps:
            return False
        
    return True

def check_correctness(id, history):
    test_data = f'./test/cost/query/{id}.json'
    
    with open(test_data, 'r', encoding='utf-8') as f:
        ground_truth = json.load(f)
    for i in range(len(history)):
        history[i]['operator'] = history[i]['action']
        history[i]['params'] = history[i]['action_input']
        history[i]['result'] = history[i]['observation']
        history[i]['current_table'] = history[i]['current_tables']
    history.insert(0, ground_truth['process'][0])
    tables_data = {}
    initial_table = ground_truth['process'][0]['initial_table']
    tables_name = [list(initial_table[j].keys())[0] for j in range(len(initial_table))]
    db_name = ground_truth['database']
    for name in tables_name:
        with open(f'../dataGeneration/data/sampled_table_1k/{db_name}/{name}.csv', 'r', encoding='utf-8') as f:
            table_df = pd.read_csv(f, low_memory=False)
            tables_data[name] = table_df
    op_tree_model = build_tree(history, tables_data)
    topo_sort_json_model = op_tree_model.tree2json()
    op_tree_base = build_tree(ground_truth['process'][:-1], tables_data)
    topo_sort_json_base = op_tree_base.tree2json()

    flag = False
    for i in range(len(topo_sort_json_model) - len(initial_table)):
        if topo_sort_json_model[i + len(initial_table)]['operator'] == 'sem_topk' or topo_sort_json_model[i + len(initial_table)]['operator'] == 'orderby':
            flag = True
            break

    if not flag:
        return True
    else:
        filter_count_model = 0
        filter_count_base = 0
        for i in range(len(topo_sort_json_model)):
            if topo_sort_json_model[i]['operator'] == 'filter' or topo_sort_json_model[i]['operator'] == 'sem_filter':
                filter_count_model += 1
            if topo_sort_json_model[i]['operator'] == 'sem_topk' or topo_sort_json_model[i]['operator'] == 'orderby':
                break
        for i in range(len(topo_sort_json_base)):
            if topo_sort_json_base[i]['operator'] == 'filter' or topo_sort_json_base[i]['operator'] == 'sem_filter':
                filter_count_base += 1
            if topo_sort_json_base[i]['operator'] == 'sem_topk' or topo_sort_json_base[i]['operator'] == 'orderby':
                break
        return filter_count_model == filter_count_base


def check_operator(model):
    result_dir = f'./result/cost/{model}/answer'
    files = os.listdir(result_dir)
    files.sort(key=lambda x: int(x.split('.')[0]))
    mismatch_file = []
    valid_file = []
    invalid_file = []
    for file in files:
        with open(f'{result_dir}/{file}', 'r', encoding='utf-8') as f:
            prediction = json.load(f)

        ground_truth_path = prediction['file_path']
        with open(ground_truth_path, 'r', encoding='utf-8') as f:
            groud_truth = json.load(f)

        ground_truth = groud_truth['process'][1:-1]
        if len(prediction['history']) > len(ground_truth):
            prediction = prediction['history'][:len(ground_truth)]
        else:
            prediction = prediction['history'][:-1]

        if check(prediction, ground_truth):
            try:
                if check_correctness(file, prediction):
                    valid_file.append(file)
                else:
                    invalid_file.append(file)
            except Exception as e:
                mismatch_file.append(file)
        else:
            mismatch_file.append(file)

    print(f"Total correct steps: {len(valid_file)} out of {len(files)}")
    return mismatch_file, valid_file, invalid_file

def calulate_cost(model, ids):
    base_dir = './result/cost/base/answer'
    model_dir = f'./result/cost/{model}/answer'
    valid_count = 0

    result = {
        'total_cost': 0,
        'prompt_tokens': 0,
        'completion_tokens': 0,
        'total_tokens': 0,
        'cache_hits': 0
    }

    for id in ids:
        base_total_cost = 0
        base_prompt_tokens = 0
        base_completion_tokens = 0
        base_total_tokens = 0
        base_cache_hits = 0
        model_total_cost = 0
        model_prompt_tokens = 0
        model_completion_tokens = 0
        model_total_tokens = 0
        model_cache_hits = 0
        try:
            with open(f'{base_dir}/{id}', 'r', encoding='utf-8') as f:
                data = json.load(f)
                history = data
                for step in history[:-1]:
                    base_total_cost += step['cost']['total_cost']
                    base_prompt_tokens += step['cost']['prompt_tokens']
                    base_completion_tokens += step['cost']['completion_tokens']
                    base_total_tokens += step['cost']['total_tokens']
                    base_cache_hits += step['cost']['cache_hits']
        except FileNotFoundError:
            continue
        with open(f'{model_dir}/{id}', 'r', encoding='utf-8') as f:
            data = json.load(f)
            history = data['history']
            for step in history[:-1]:
                model_total_cost += step['cost']['total_cost']
                model_prompt_tokens += step['cost']['prompt_tokens']
                model_completion_tokens += step['cost']['completion_tokens']
                model_total_tokens += step['cost']['total_tokens']
                model_cache_hits += step['cost']['cache_hits']

        valid_count += 1
        result['total_cost'] += model_total_cost / base_total_cost if base_total_cost > 0 else 0
        result['prompt_tokens'] += model_prompt_tokens / base_prompt_tokens if base_prompt_tokens > 0 else 0
        result['completion_tokens'] += model_completion_tokens / base_completion_tokens if base_completion_tokens > 0 else 0
        result['total_tokens'] += model_total_tokens / base_total_tokens if base_total_tokens > 0 else 0
        result['cache_hits'] += model_cache_hits / base_cache_hits if base_cache_hits > 0 else 0

    result['total_cost'] /= valid_count
    result['prompt_tokens'] /= valid_count
    result['completion_tokens'] /= valid_count
    result['total_tokens'] /= valid_count
    result['cache_hits'] /= valid_count

    return result

def main():
    models = [
        'DeepSeek-V3',
    ]
    mismatch_files = []
    valid_files = []
    invalid_files = []
    for model in models:
        mismatch, valid, invalid = check_operator(model)
        mismatch_files.append(mismatch)
        valid_files.append(valid)
        invalid_files.append(invalid)

    valid_sum = 0

    for i in range(len(models)):
        model = models[i]
        print(f"Calculating cost for model: {model}")
        result = calulate_cost(model, valid_files[i])
        print(f"Results for {model}:")
        print(f"Total Cost: {result['total_cost']:.2f}")
        print(f"Prompt Tokens (ratio): {result['prompt_tokens']:.2f}")
        print(f"Completion Tokens (ratio): {result['completion_tokens']:.2f}")
        print(f"Total Tokens (ratio): {result['total_tokens']:.2f}")
        print(f"Cache Hits: {result['cache_hits']:.2f}")
        print(f"Valid Rate: {len(valid_files[i]) / (len(invalid_files[i]) + len(valid_files[i])) * 100:.2f}%")
        valid_sum += len(valid_files[i]) / (len(invalid_files[i]) + len(valid_files[i])) * 100

        print("----------------------------------")

    print(f"Average Valid Rate: {valid_sum / len(models):.2f}%")

if __name__ == "__main__":
    main()