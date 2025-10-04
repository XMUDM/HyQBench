import os
import json
from tqdm import tqdm
import traceback
from scripts.data_completion import add_query, add_thought

file_to_process = []

for type in ['flat', 'nested']:
    for root, dirs, files in os.walk(f'./result/{type}/answer'):
        for file in files:
            if file.endswith('.json'):
                file_to_process.append(os.path.join(root, file))

for i in tqdm(range(len(file_to_process))):
    file_path = file_to_process[i]
    print(f"Processing {file_path}...")
    result_path = file_path.replace('answer', 'completed')
    if os.path.exists(result_path):
        print(f"Already processed {file_path}")
        continue
    with open(file_path, 'r', encoding='utf-8') as f:
        process = json.load(f)
    raw_file_path = file_path.replace('answer', 'optimized')
    with open(raw_file_path, 'r', encoding='utf-8') as f:
        raw_process = json.load(f)
    process.insert(0, raw_process[0])
    result = {}
    try:
        result['query'] = add_query(process)
    except Exception as e:
        print(f"Error adding query for {file_path}: {e}")
        traceback.print_exc()
        continue
    try:
        thought = add_thought(result['query'], process)
    except Exception as e:
        print(f"Error getting thought for {file_path}: {e}")
        traceback.print_exc()
        continue
    try:
        for j in range(1, len(process)):
            process[j]['Thought'] = thought[j - 1]['Thought']
        process.append(thought[-1])
    except Exception as e:
        print(f"Error adding thought for {file_path}: {e}")
        traceback.print_exc()
        continue
    result['process'] = process
    if not os.path.exists(os.path.dirname(result_path)):
        os.makedirs(os.path.dirname(result_path))
    try:
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error writing result for {file_path}: {e}")
        traceback.print_exc()
        continue
            