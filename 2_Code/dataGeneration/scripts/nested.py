import sys
import os
import json
from .tool_pool import tools
from .table_processing import apply_operator
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.llm import DP

class nestedGenerator:
    def __init__(self, tables_data, history):
        self.tables_data = tables_data
        # record the whole process
        self.history = history
        self.total_tools = tools
        self.model = DP()
        
     # generate the next steps based on the history of the subquery
    def get_steps(self, history):
        print(json.dumps(history, indent=4))
        prompt = f"""
I am a database teacher, and now I want to create some questions to test. The format of the questions is to provide some tables and some operators, and by selecting the appropriate operators and providing reasonable parameters to process the tables, the correct answer can be obtained.

Operators include value_operator and semantic_operator. value_operator is used for operations that can directly get results from the table, while semantic_operators starting with 'sem_' are used for operations that require combining external knowledge. For example, when the table lacks common sense information such as location or gender, making all columns unable to answer, or when semantic reasoning is needed to get the result.

Now I want to generate some solution processes. The query type is a nested query that includes subqueries. I have already generated the execution process for the subquery. Please continue with the process I provided and give three nested queries that can be generated based on the subquery, along with the next steps for each. For each possible nested query, provide no more than four reasonable new steps. Try to make all operations meaningful.

All operators are as follows:
{json.dumps(self.total_tools, indent=4)}

The subquery processing procedures are as follows:
{json.dumps(history, indent=4)}

The following are some precautions:
1. You must use the result of the final step in the execution of the subquery. If the subquery result is a table, you must process this table further. If the result is a number, you must use this number in subsequent operations.
2. You just need to give the plan, not the parameters.
3. All the steps should ultimately be summarized into a query, with the previous ones just being intermediate processes. There should not be multiple answers.
4. Try to use all the initial tables as much as possible.
5. Try to use a semantic operator in each query.

You must provide the steps in the following format. Your answer should start from "[". Do not add any other content:
[
    {{
        "Query": "The first possible nested query",
        "steps": [
            {{
                "Thought": "your thought of the operator",
                "Operator": "the reasonable and accurate Operator name"
            }},
            ...
        ],
        "rationality": "your score of the rationality of the query (1-10)"
    }},
    {{
        "Query": "The second possible nested query",
        "steps": [
            {{
                "Thought": "your thought of the operator",
                "Operator": "the reasonable and accurate Operator name"
            }},
            ...
        ],
        "rationality": "your score of the rationality of the query (1-10)"
    }},
    {{
        "Query": "The third possible nested query",
        "steps": [
            {{
                "Thought": "your thought of the operator",
                "Operator": "the reasonable and accurate Operator name"
            }},
            ...
        ],
        "rationality": "your score of the rationality of the query (1-10)"
    }},
]
    """
        print(prompt)
        response = self.model(prompt)
        print(response)
        response = eval(response)
        # select the most reasonable one
        response = sorted(response, key=lambda x: x['rationality'], reverse=True)[0]['steps']
        return response

    # generate the parameters for the chosen operator
    def get_operator_params(self, operator, thought, history, all_operators):
        print(json.dumps(history, indent=4))
        prompt = f"""
I am a database teacher, and now I want to create some questions to test. The format of the questions is to provide some tables and some operators, and by selecting the appropriate operators and providing reasonable parameters to process the tables, the correct answer can be obtained.

Operators include value_operator and semantic_operator. value_operator is used for operations that can directly get results from the table, while semantic_operators starting with 'sem_' are used for operations that require combining external knowledge. For example, when the table lacks common sense information such as location or gender, making all columns unable to answer, or when semantic reasoning is needed to get the result.

Now I want to generate some solution processes. I have confirmed the operations to be used and the previous processing procedures. Please generate reasonable inputs for the new operations I have chosen. Please try to make all operations meaningful.

All operators are as follows:
{json.dumps(self.total_tools, indent=4)}

The previous processing procedures are as follows:
{json.dumps(history, indent=4)}

The following are some precautions:
1. If the chosen operator is filter or sem_filter, please try to process different columns and the original table. If the operator is join or sem_join, if the original table has already been processed, please try to join the processed table. If not, join the original table.For other operators, continue processing on the results from the previous step to ensure the processing chain is complete.
2. Note that semantic operators can automatically combine external internet knowledge and basic common sense to process data. Therefore, for the parameters of semantic operators, you can set some content that cannot be directly obtained from tables but can be processed by combining external knowledge. Do not use semantic operators for simple value comparisons. 
3. The column names in parentheses must match the table.
4. If the operator is sem_join, ensure that the parameter format is correct. The parameters should only contain two columns, each located in a different table.
5. If the operator is sem_agg, Please note the sections of the test and generate the relevant questions.

The new operator I have chosen is: {operator}
The purpose of this operator is: {thought}
    
You must provide the parameters in the following format.  Your answer should start from "{{". Do not add any other content:
{{
    "Thought": "your thought of the parameters", 
    "Parameters": "the reasonable and accurate parameters"
}}
    """
        response = self.model(prompt)
        response = eval(response)
        return response

    def generate(self):
        next_steps = self.get_steps(self.history)
        all_operators = [step['Operator'] for step in next_steps]
        for step in next_steps:
            operator = step['Operator']
            thought = step['Thought']
            params = self.get_operator_params(operator, thought, self.history, all_operators)
            params = params['Parameters']
            res, self.tables_data = apply_operator(operator, params, self.tables_data)
            if 'Empty table' in res or 'Invalid' in res:
                return res, self.tables_data
            self.history.append({
                'operator': operator,
                'params': params,
                'result': res,
                'current_table': [name for name in self.tables_data.keys()]
            })
        
        return self.history, self.tables_data
            