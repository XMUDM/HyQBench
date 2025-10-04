import os
import json
import pandas as pd
import sys
from .evaluate_lotus import evaluate_table, evaluate_string
sys.path.append('..')
from utils.llm import GPT

def answer_accuracy(prediction, ground_truth, ground_truth_table_dir):
    print("Evaluating answer accuracy...")
    if ground_truth[-2]['operator'] == 'sem_agg':
        print("Evaluating sem_agg operator...")
        ground_truth = str(ground_truth[-1]['Final Answer'])
        # prediction取Answer列第一行
        prediction = prediction.iloc[0]['Answer'] if 'Answer' in prediction.columns else prediction.iloc[0][0]

        return evaluate_string(prediction, ground_truth)

    elif ground_truth[-2]['operator'] in ['agg', 'value_calculate']:
        print("Evaluating agg or value_calculate operator...")
        try:
            ground_truth = str(ground_truth[-2]['result'])
            print(f"Ground truth: {ground_truth}, Prediction: {prediction}")
            prediction = str(prediction.iloc[0][0])
            return ground_truth == prediction
        except Exception as e:
            return False
    else:
        try:
            print("Evaluating other operators...")
            ground_truth_table_name = ground_truth[-2]['current_table'][-1]
            ground_truth = pd.read_csv(os.path.join(ground_truth_table_dir, ground_truth_table_name + '.csv'), low_memory=False)
        except Exception as e:
            return False

        return evaluate_table(prediction, ground_truth)

def evaluate_case(path, test_cases):
    detail = []
    correct_count = 0

    for i in range(len(test_cases)):
        prediction_path = test_cases[i]
        print(f"Processing test case {i + 1}: {prediction_path}...")
        try:
            with open(prediction_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            ground_truth_path = data['file_path']
            table_path = prediction_path.replace('answer', 'table')[:-5] + '.csv'
            prediction = pd.read_csv(table_path, low_memory=False)

            with open(ground_truth_path, 'r', encoding='utf-8') as f:
                ground_truth_data = json.load(f)
            ground_truth = ground_truth_data['process']

            ground_truth_table_dir = ground_truth_path.replace('query', 'table')[:-5]

            answer_accuracy_result = answer_accuracy(prediction, ground_truth, ground_truth_table_dir)
            if answer_accuracy_result:
                correct_count += 1

            detail.append({
                'test_case': test_cases[i],
                'answer_accuracy': answer_accuracy_result
            })
            
        except Exception as e:
            print(f"Error processing {test_cases[i]}: {e}")
        
        with open(f'{path}/evaluate.json', 'w', encoding='utf-8') as f:
            json.dump(detail, f, indent=4, ensure_ascii=False)

    with open(f'{path}/evaluate.json', 'w', encoding='utf-8') as f:
        json.dump(detail, f, indent=4, ensure_ascii=False)

def evaluate(model):
    test_cases = []
    path = f'./result/blendsql/{model}'
    for file in os.listdir(f'{path}/answer'):
        if file.endswith('.json'):
            test_cases.append(os.path.join(f'{path}/answer', file))

    print(len(test_cases), "test cases found.")
    test_cases.sort(key=lambda x: int(os.path.basename(x).split('.')[0]))
    evaluate_case(path, test_cases)

def analyze(detail):
    count = {
        '1': 143,
        '2': 143,
        '3': 143,
        '4': 143,
        '5': 143,
        '6': 143,
        '7': 142
    }
    correct = {
        '1': 0,
        '2': 0,
        '3': 0,
        '4': 0,
        '5': 0,
        '6': 0,
        '7': 0
    }
    success_count = correct.copy()
    success_rate = correct.copy()

    for item in detail:
        file_name = item['test_case']
        id = int(file_name.split('/')[-1].split('.')[0])
        # Each has extracted 143 samples.
        ground_truth_op_count = (id - 1) // 143 + 1
        success_count[str(ground_truth_op_count)] += 1
        if item['answer_accuracy'] == True:
           correct[str(ground_truth_op_count)] += 1
    for key in correct:
        success_rate[key] = success_count[key] / count[key]
        if count[key] != 0:
            correct[key] = round(correct[key] / count[key], 4)
        else:
            correct[key] = 0.0
    return correct, success_rate

def get_total_result(path):
    with open(path, 'r', encoding='utf-8') as f:
        detail = json.load(f)
    correct, success_rate = analyze(detail)
    sum = 0
    sum_success_rate = 0
    for key, value in correct.items():
        sum += value
        sum_success_rate += success_rate[key]
    print(f'  Success Rate: {round(sum_success_rate / 7, 4)}')
    print(f'  Accuracy: {round(sum / 7, 4)}')

if __name__ == "__main__":
    model = "DeepSeek-V3"
    evaluate(model)
    get_total_result(f'./result/blendsql/{model}/evaluate.json')
