import sys
import os
import json
import random
from .classify import tool
from .tool_pool import tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.llm import DP
from .table_processing import get_tables_info, apply_operator

class generator:
    def __init__(self, tool_type_list, tables_data, table_des, result_path, table_path, query_type):
        self.query_type = query_type
        self.tool_type_list = tool_type_list
        self.tables_data = tables_data
        self.table_des = table_des
        self.max_depth = len(tool_type_list)
        self.result_path = result_path
        self.table_path = table_path
        self.continue_count = 0
        # record the number of generated data
        self.data_count = 0
        # record the whole generation process
        self.history = [{'initial_table': get_tables_info(self.tables_data, self.table_des)}]
        self.total_tools = {}
        self.model = DP()

    # generate parameters for the operator using a large language model
    def get_operator_params(self, operator, history, round):
        print(json.dumps(history, indent=4))
        prompt = f"""
I am a database teacher, and now I want to create some questions to test. The format of the questions is to provide some tables and some operators, and by selecting the appropriate operators and providing reasonable parameters to process the tables, the correct answer can be obtained.

Operators include value_operator and semantic_operator. value_operator is used for operations that can directly get results from the table, while semantic_operators starting with 'sem_' are used for operations that require combining external knowledge. For example, when the table lacks common sense information such as location or gender, making all columns unable to answer, or when semantic reasoning is needed to get the result.

Now I want to generate some solution processes. I have confirmed the operations to be used and the previous processing procedures. Please generate reasonable inputs for the new operations I have chosen. Please try to make all operations meaningful.

All operators are as follows:
{json.dumps(self.total_tools, indent=4)}

The previous processing procedures are as follows:
{json.dumps(history, indent=4)}

The current problem is testing the chapter: {self.query_type}
A total of {self.max_depth - self.continue_count} times need to process the tables and this is the {round + 1 - self.continue_count} processing.

The following are some precautions:
1. If the chosen operator is filter or sem_filter, please try to process different columns and the original table. If the operator is join or sem_join, if the original table has already been processed, please try to join the processed table. If not, join the original table.For other operators, continue processing on the results from the previous step to ensure the processing chain is complete.
2. Note that semantic operators can automatically combine external internet knowledge and basic common sense to process data. Therefore, for the parameters of semantic operators, you can set some content that cannot be directly obtained from tables but can be processed by combining external knowledge. Do not use semantic operators for simple value comparisons. 
3. The column names in parentheses must match the table.
4. If the operator is sem_join, ensure that the parameter format is correct. The parameters should only contain two columns, each located in a different table.
5. If the operator is sem_agg, Please note the sections of the test and generate the relevant questions.

The new operator I have chosen is: {operator}

Please provide **four diverse parameter suggestions** based on the previous process and the data types and ranges in the current tables. Your generation should have greater randomness. 
    
You must provide the parameters in the following format.  Your answer should start from "[". Do not add any other content:
"""
        if round + 1 - self.continue_count != 1:
            selected_table = self.history[-1]['current_table'][-1]
            prompt += f"""
[
    {{
        "Thought": "your first idea",
        "Parameters": "{selected_table};;..."
    }},
    {{
        "Thought": "a better second idea",
        "Parameters": "{selected_table};;..."
    }},
    {{
        "Thought": "a different third idea",
        "Parameters": "{selected_table};;..."
    }},
    {{
        "Thought": "a different fourth idea",
        "Parameters": "{selected_table};;..."
    }}
]
    """
        else:
            prompt += f"""
[
    {{
        "Thought": "your first idea",
        "Parameters": "the reasonable and accurate parameters"
    }},
    {{
        "Thought": "a better second idea",
        "Parameters": "the reasonable and accurate parameters"
    }},
    {{
        "Thought": "a different third idea",
        "Parameters": "the reasonable and accurate parameters"
    }},
    {{
        "Thought": "a different fourth idea",
        "Parameters": "the reasonable and accurate parameters"
    }}
]
    """

        print(prompt)
        response = self.model(prompt)
        print(response)
        response_list = eval(response)

        return response_list[1:]
    
        # generate parameters for join operator using a large language model (join operator)
    def get_operator_params_join(self, operator, history, round):
        prompt = f"""
I am a database teacher, and now I want to create some questions to test. The format of the questions is to provide some tables and some operators, and by selecting the appropriate operators and providing reasonable parameters to process the tables, the correct answer can be obtained.

Operators include value_operator and semantic_operator. value_operator is used for operations that can directly get results from the table, while semantic_operators starting with 'sem_' are used for operations that require combining external knowledge. For example, when the table lacks common sense information such as location or gender, making all columns unable to answer, or when semantic reasoning is needed to get the result.

Now I want to generate some solution processes. I have confirmed the operations to be used and the previous processing procedures. Please generate reasonable inputs for the new operations I have chosen. Please try to make all operations meaningful.

All operators are as follows:
{json.dumps(self.total_tools, indent=4)}

The previous processing procedures are as follows:
{json.dumps(history, indent=4)}

The current problem is testing the chapter: {self.query_type}
A total of {self.max_depth - self.continue_count} times need to process the tables and this is the {round + 1 - self.continue_count} processing.

The following are some precautions:
1. If the chosen operator is filter or sem_filter, please try to process different columns and the original table. If the operator is join or sem_join, if the original table has already been processed, please try to join the processed table. If not, join the original table.For other operators, continue processing on the results from the previous step to ensure the processing chain is complete.
2. Note that semantic operators can automatically combine external internet knowledge and basic common sense to process data. Therefore, for the parameters of semantic operators, you can set some content that cannot be directly obtained from tables but can be processed by combining external knowledge. Do not use semantic operators for simple value comparisons. 
3. The column names in parentheses must match the table.
4. If the operator is sem_join, ensure that the parameter format is correct. The parameters should only contain two columns, each located in a different table.
5. If the operator is sem_agg, Please note the sections of the test and generate the relevant questions.

The new operator I have chosen is: {operator}
    
You must provide the parameters in the following format.  Your answer should start from "{{". Do not add any other content:
"""
        if round + 1 - self.continue_count != 1:
            selected_table = self.history[-1]['current_table'][-1]
            prompt += f"""
{{
    "Thought": "your thought of the parameters",        
    "Parameters": "{selected_table};;..."
}}
    """
        else:
            prompt += f"""
{{
    "Thought": "your thought of the parameters", 
    "Parameters": "the reasonable and accurate parameters"
}}
    """
        response = self.model(prompt)
        response = eval(response)
        return response

    # dfs search to generate data
    def dfs_execute(self):
        valid_types = self.tool_type_list  # get valid tool types for this query type
        # get all tools that can be used in this query type
        for it_type in valid_types:
            for op in tool[it_type]:
                if op == 'continue':
                    continue
                self.total_tools[op] = tools[op]

        # depth-first search
        def dfs(current_depth, current_data):
            if current_depth == self.max_depth:
                self.data_count += 1
                # save the generated data and process
                if not os.path.exists(self.result_path):
                    os.makedirs(self.result_path)
                with open(f'{self.result_path}/{self.data_count}.json', 'w', encoding='utf-8') as f:
                    json.dump(self.history, f, ensure_ascii=False, indent=4)
                # remove previous data if exists
                if os.path.exists(f'{self.result_path}/{self.data_count}'):
                    for file in os.listdir(f'{self.result_path}/{self.data_count}'):
                        os.remove(os.path.join(f'{self.result_path}/{self.data_count}', file))
                if not os.path.exists(f'{self.table_path}/{self.data_count}'):
                    os.makedirs(f'{self.table_path}/{self.data_count}')
                for table_name in current_data:
                    current_data[table_name].to_csv(f'{self.table_path}/{self.data_count}/{table_name}.csv', index=False)
                print(f'New data generated: \n{json.dumps(self.history, indent=4)}')
                return "New data generated."
            
            # try all operators of the current type
            for operator in tool[valid_types[current_depth]]:
                if operator == 'continue':
                    self.continue_count += 1
                    if 'No suitable parameters.' in dfs(current_depth + 1, current_data):
                        return "No suitable parameters."
                    self.continue_count -= 1
                    
                else:
                    if operator == 'join':
                        # generate parameters for join operator
                        params = self.get_operator_params_join(operator, self.history, current_depth)
                    elif operator == 'head':
                        params = [{'Parameters': 'head;;' + str(random.randint(1, min(6, len(current_data[self.history[-1]["current_table"][-1]]))))}]
                    else:
                        # generate parameters for other operators
                        params = self.get_operator_params(operator, self.history, current_depth)

                    if type(params) != list:
                        params = [params]
                    
                    # randomize the order of parameters to increase diversity
                    random.shuffle(params)

                    success = False
                    for param in params:
                        param = param['Parameters']

                        if operator != 'value_calculate' and len(self.history) > 1 and param.split(';;')[0] != self.history[-1]['current_table'][-1]:
                            continue

                        # apply the operator with the generated parameters
                        res, output_data = apply_operator(operator, param, current_data)

                        if res == 'result: Empty table' or 'Invalid' in res:
                            if res == 'result: Empty table':
                                print(f'Result of {operator} is empty.')
                            elif 'Invalid arguments: Numeric column should not be applied to semantic operators' in res:
                                print("Invalid arguments: Numeric column should not be applied to semantic operators'")
                                return "No suitable parameters."
                            else:
                                print(f'Invalid operator {operator} with params {param}. Error message: {res}.')
                            continue
                            
                        else:
                            success = True
                            break
                    
                    if not success:
                        print(f'No suitable parameters for {operator}.')
                        continue

                    # record the operation and its result
                    self.history.append({
                        'operator': operator,
                        'params': param,
                        'result': res,
                        'current_table': [name for name in output_data.keys()]
                    })

                    if 'No suitable parameters.' in dfs(current_depth + 1, output_data):
                        return "No suitable parameters."

                    # backtrack
                    self.history.pop()

            return "Finished all operations."

        dfs(0, self.tables_data)
