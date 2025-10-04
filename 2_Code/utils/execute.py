import json
import pandas as pd
import sys
sys.path.append('..')
from dataGeneration.scripts.tool import callTool
from dataGeneration.scripts.tool_pool import tools
from .llm import DP

class executor:
    def __init__(self, table_name, tool_call, tool_call_arguments, query):
        self.table_name = table_name
        self.tool_call = tool_call
        self.tool_call_arguments = tool_call_arguments
        self.query = query
        self.model = DP()

    # Reflect on whether the input for the tool call meets the requirements
    def reflect(self, data, error_message=None):
        tables = {}
        for table in data:
            columns = {'*': 'all columns'}
            for column in data[table]:
                if len(data[table][column].drop_duplicates()) <= 6:
                    columns[column] = 'dtype=' + str(data[table][column].dtype) + ' e.g. ' + '; '.join([str(x) for x in data[table][column].drop_duplicates().tolist()])
                else:
                    columns[column] = 'dtype=' + str(data[table][column].dtype) + ' e.g. ' + '; '.join([str(x) for x in data[table][column].drop_duplicates().sample(6).tolist()])
            tables[table] = columns
        if self.table_name == '*':
            temp_arguments = ';;'.join(self.tool_call_arguments)
        else:
            temp_arguments = self.table_name + ';;' + ';;'.join(self.tool_call_arguments)
        role_prompt = f"""
You are a database expert, required to check the name and the input arguments of the tool. The arguments of tools are separated by ";;". Please note that the input of semantic operators must include column names enclosed in curly braces. The all tools and the arguments of them should be in the following format:

{json.dumps(tools, indent=4)}

The table data used is as follows:

{json.dumps(tables, indent=4)}

The tool is one step towards solving the following query:
{self.query}

The now calling tool is {self.tool_call} with the arguments: {temp_arguments}.

        """

        error_prompt = f"""
The call of the tool failed. The error message is as follows:
{error_message}

        """

        instruction_prommpt = f"""
Please check the name and the input arguments of the tool. The input must be completely in accordance with the requirements. If they are correct, please only output true. Otherwise, please output false and correct errors. The format of the output should be as follows:
{{
    "Thought": "The thought about the tool call",
    "result": "true/false",
    "name": "correct tool name", (Note that only output when the result is false)
    "arguments": "correct arguments", e.g. "arg1;;arg2;;arg3" (Note that only output when the result is false)
}}
        """



        if error_message:
            prompt = role_prompt + error_prompt + instruction_prommpt
        else:
            prompt = role_prompt + instruction_prommpt

        response = self.model(prompt)

        correct = response.split('"result": "')[1].split('"')[0]
        if correct == 'false':
            new_tool = response.split('"name": "')[1].split('"')[0]
            self.tool_call = new_tool
            new_arguments = response.split('"arguments": "')[1].split('"')[0]
            self.tool_call_arguments = new_arguments
            self.tool_call_arguments = [arg.strip() for arg in self.tool_call_arguments.split(";;")]
            if self.tool_call == 'value_calculate':
                self.table_name = '*'
            else:
                self.table_name = self.tool_call_arguments[0]
                self.tool_call_arguments = self.tool_call_arguments[1:]

    def execute(self, table_data):
        max_attempts = 3
        has_failed = False
        while True:
            try:
                res_chain = callTool(table_data, self.table_name, self.tool_call, self.tool_call_arguments)
                if not isinstance(res_chain, str):
                    if isinstance(res_chain, pd.DataFrame):
                        if res_chain.empty:
                            res_chain = 'result: Empty table'
                        else:
                            if f"{self.table_name}_{self.tool_call}" in table_data:
                                for i in range(2, 10):
                                    if f"{self.table_name}_{self.tool_call}_{i}" not in table_data:
                                        temp_name = f"{self.table_name}_{self.tool_call}_{i}"
                                        table_data[temp_name] = res_chain
                                        break
                            else:
                                temp_name = f"{self.table_name}_{self.tool_call}"
                                table_data[temp_name] = res_chain

                            columns = {'*': 'all columns'}
                            for column in res_chain:
                                if len(res_chain[column].drop_duplicates()) <= 6:
                                    columns[column] = 'dtype=' + str(res_chain[column].dtype) + ' e.g. ' + '; '.join([str(x) for x in res_chain[column].drop_duplicates().tolist()])
                                else:
                                    columns[column] = 'dtype=' + str(res_chain[column].dtype) + ' e.g. ' + '; '.join([str(x) for x in res_chain[column].drop_duplicates().sample(6).tolist()])
                            # columns = ["*", *res_chain.columns.tolist()]
                            res_chain = f'{temp_name}: ' + str(columns) + ' table_length: ' + str(res_chain.shape[0])
                    else:
                        res_chain = 'result: ' + str(res_chain)
                    break
                else:
                    max_attempts -= 1
                    has_failed = True
                    if max_attempts <= 0:
                        res_chain = "Failed to execute. Error message: " + res_chain
                        break
                    self.reflect(table_data, res_chain)
            except Exception as e:
                max_attempts -= 1
                has_failed = True
                if max_attempts <= 0:
                    res_chain = "Failed to execute. Error message: " + str(e)
                    break
                self.reflect(table_data, str(e))

        return self.table_name, self.tool_call, self.tool_call_arguments, res_chain, table_data, has_failed
