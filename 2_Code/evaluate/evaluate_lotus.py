import os
import json
import pandas as pd
from copy import deepcopy
import sys
from .analysis import get_total_result
sys.path.append('..')
from utils.llm import GPT

def evaluate_string(prediction, ground_truth):
    model = GPT(model="gpt-4.1")
    prompt = f"""You are a judge evaluating the correctness of a prediction against a ground truth.
The prediction and ground truth are both strings. Your task is to determine if the prediction is correct.
### Prediction
{prediction}

### Ground Truth
{ground_truth}

If there is a factual error, the answer is False, otherwise it is True, no other aspects need to be considered. Your answer should follow the following format:
{{
    "reason": "Your reason for the judgment",
    "result": "True" or "False"
}}
    """
    response = model(prompt)
    try:
        response = eval(response)
    except Exception as e:
        response = response.replace('```json', '').replace('```', '').strip()
        response = eval(response)
    if response['result'] == False or response['result'] == 'False':
        print(f"Reason: {response['reason']}")
        return False
    return True

def evaluate_table(prediction, ground_truth):
    prediction.columns = [col.replace(':left', '').replace(':right', '_1') for col in prediction.columns]
    ground_truth.columns = [col.replace(':left', '').replace(':right', '_1') for col in ground_truth.columns]
    # Find common columns
    common_columns = list(set(prediction.columns) & set(ground_truth.columns))
    if not common_columns:
        print("No common columns found between prediction and ground truth.")
        return False

    prediction = prediction[common_columns]
    ground_truth = ground_truth[common_columns]

    prediction = prediction.drop_duplicates().reset_index(drop=True)
    ground_truth = ground_truth.drop_duplicates().reset_index(drop=True)

    count = 0
    # Check if each row in prediction exists in ground_truth
    for i in range(len(prediction)):
        row = prediction.iloc[i]
        row_exists = False
        for j in range(len(ground_truth)):
            if row.equals(ground_truth.iloc[j]):
                row_exists = True
                break
        if row_exists:
            count += 1
    
    if count / len(prediction) < 0.9 or count / len(ground_truth) < 0.9:
        print("Prediction does not match ground truth.")
        return False
    
    return True

def operator_selection_accuracy(prediction, ground_truth):
    print("Evaluating operator selection accuracy...")
    prediction_selection = [item['action'] for item in prediction[:-1]]
    ground_truth_selection = [item['operator'] for item in ground_truth[1:-1]]
    prediction_count = len(prediction_selection)
    ground_truth_count = len(ground_truth_selection)
    print(f"Prediction count: {prediction_count}, Ground truth count: {ground_truth_count}")
    correct_count = 0
    for i in range(len(prediction_selection)):
        if prediction_selection[i] in ground_truth_selection:
            correct_count += 1
            ground_truth_selection.remove(prediction_selection[i])
    precision = correct_count / prediction_count if prediction_count > 0 else 0
    recall = correct_count / ground_truth_count if ground_truth_count > 0 else 0
    return precision, recall

def operator_format_accuracy(prediction):
    success_count = 0
    for item in prediction[:-1]:
        if item['has_failed'] == False:
            success_count += 1
    operator_count = len(prediction) - 1
    return operator_count, success_count

def answer_accuracy(prediction, ground_truth, prediction_table_dir, ground_truth_table_dir):
    print("Evaluating answer accuracy...")
    if ground_truth[-2]['operator'] == 'sem_agg':
        print("Evaluating sem_agg operator...")
        ground_truth = str(ground_truth[-1]['Final Answer'])
        prediction = str(prediction[-1]['final answer'])

        return evaluate_string(prediction, ground_truth)

    elif ground_truth[-2]['operator'] in ['agg', 'value_calculate']:
        print("Evaluating agg or value_calculate operator...")
        try:
            raw_ground_truth = deepcopy(ground_truth)
            raw_prediction = deepcopy(prediction)
            ground_truth_table_name = ground_truth[-2]['current_table'][-1]
            prediction_table_name = prediction[-2]['current_tables'][-1]
            print(f"Ground truth table: {ground_truth_table_name}, Prediction table: {prediction_table_name}")
            ground_truth = pd.read_csv(os.path.join(ground_truth_table_dir, ground_truth_table_name + '.csv'), low_memory=False)
            prediction = pd.read_csv(os.path.join(prediction_table_dir, prediction_table_name + '.csv'), low_memory=False)

            return evaluate_table(prediction, ground_truth) or evaluate_string(raw_prediction[-1]['final answer'], raw_ground_truth[-1]['Final Answer'])
        except Exception as e:
            return False

    else:
        try:
            print("Evaluating other operators...")
            raw_ground_truth = deepcopy(ground_truth)
            raw_prediction = deepcopy(prediction)
            ground_truth_table_name = ground_truth[-2]['current_table'][-1]
            prediction_table_name = prediction[-2]['current_tables'][-1]
            print(f"Ground truth table: {ground_truth_table_name}, Prediction table: {prediction_table_name}")
            ground_truth = pd.read_csv(os.path.join(ground_truth_table_dir, ground_truth_table_name + '.csv'), low_memory=False)
            prediction = pd.read_csv(os.path.join(prediction_table_dir, prediction_table_name + '.csv'), low_memory=False)
        except Exception as e:
            return False
        
        return evaluate_table(prediction, ground_truth)

def evaluate_case(test_cases, model):
    detail = []
    correct_count = 0
    precision_sum = 0
    recall_sum = 0
    operator_total_count = 0
    success_total_count = 0

    for i in range(len(test_cases)):
        prediction_path = test_cases[i]
        print(f"Processing test case {i + 1}: {prediction_path}...")
        try:
            with open(prediction_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            ground_truth_path = data['file_path']
            prediction = data['history']

            with open(ground_truth_path, 'r', encoding='utf-8') as f:
                ground_truth_data = json.load(f)
            ground_truth = ground_truth_data['process']

            prediction_table_dir = prediction_path.replace('answer', 'table')[:-5]
            ground_truth_table_dir = ground_truth_path.replace('query', 'table')[:-5]

            precision, recall = operator_selection_accuracy(prediction, ground_truth)
            precision_sum += precision
            recall_sum += recall

            answer_accuracy_result = answer_accuracy(prediction, ground_truth, prediction_table_dir, ground_truth_table_dir)
            if answer_accuracy_result:
                correct_count += 1

            operator_count, success_count = operator_format_accuracy(prediction)
            operator_total_count += operator_count
            success_total_count += success_count

            detail.append({
                'test_case': test_cases[i],
                'precision': precision,
                'recall': recall,
                'answer_accuracy': answer_accuracy_result,
                'operator_count': operator_count,
                'success_count': success_count,
            })
            
        except Exception as e:
            print(f"Error processing {test_cases[i]}: {e}")

    with open(f'./result/lotus/{model}/evaluate.json', 'w', encoding='utf-8') as f:
        json.dump(detail, f, indent=4, ensure_ascii=False)

def evaluate(model):
    test_cases = []
    for file in os.listdir(f'./result/lotus/{model}/answer'):
        if file.endswith('.json'):
            test_cases.append(os.path.join(f'./result/lotus/{model}/answer', file))

    test_cases.sort(key=lambda x: int(os.path.basename(x).split('.')[0]))
    evaluate_case(test_cases, model)

def analyze(detail):
    template = {
        '1': 0,
        '2': 0,
        '3': 0,
        '4': 0,
        '5': 0,
        '6': 0,
        '7': 0
    }

    count = template.copy()
    correct = template.copy()
    precision = template.copy()
    recall = template.copy()
    op_count = template.copy()
    success_count = template.copy()


    for item in detail:
        file_name = item['test_case']
        id = int(file_name.split('/')[-1].split('.')[0])
        # Each has extracted 143 samples.
        ground_truth_op_count = (id - 1) // 143 + 1
        if item['answer_accuracy'] == True:
           correct[str(ground_truth_op_count)] += 1
        count[str(ground_truth_op_count)] += 1
        precision[str(ground_truth_op_count)] += item['precision']
        recall[str(ground_truth_op_count)] += item['recall']
        op_count[str(ground_truth_op_count)] += item['operator_count']
        success_count[str(ground_truth_op_count)] += item['success_count']
    for key in correct:
        if count[key] != 0:
            correct[key] = round(correct[key] / count[key], 4)
            precision[key] = round(precision[key] / count[key], 4)
            recall[key] = round(recall[key] / count[key], 4)
            success_count[key] = round(success_count[key] / op_count[key], 4)
            op_count[key] = round(op_count[key] / count[key], 4)
        else:
            correct[key] = 0.0
            precision[key] = 0.0
            recall[key] = 0.0
            op_count[key] = 0.0
            success_count[key] = 0.0
    return correct, precision, recall, op_count, success_count

def get_total_result(path):
    with open(path, 'r', encoding='utf-8') as f:
        detail = json.load(f)
    correct, precision, recall, op_count, success_count = analyze(detail)
    sum = 0
    precision_sum = 0
    recall_sum = 0
    op_count_sum = 0
    success_count_sum = 0
    for key, value in correct.items():
        sum += value
        precision_sum += precision[key]
        recall_sum += recall[key]
        op_count_sum += op_count[key]
        success_count_sum += success_count[key]
    print(f'  Accuracy: {round(sum / 7, 4)}')
    print(f'  Precision: {round(precision_sum / 7, 4)}')
    print(f'  Recall: {round(recall_sum / 7, 4)}')
    print(f'  Average op count: {round(op_count_sum / 7, 2)}')
    print(f'  success count: {round(success_count_sum / 7, 4)}')

if __name__ == "__main__":
    model = "DeepSeek-V3"
    evaluate(model)
    get_total_result(f'./result/lotus/{model}/evaluate.json')
