from .preprocess import preprocessor
from .parse import parser
from .plan import planner
from .preprocess_cost import preprocessor_cost
from .plan_cost import planner_cost
from .execute import executor
import json
from dataGeneration.scripts.table_processing import get_tables_info
import os
import sys
sys.path.append('..')
import lotus

class solver:
    def __init__(self, model, query, tables, table_data, description=None, steps=None):
        if steps:
            self.preprocessor = preprocessor_cost()
        else:
            self.preprocessor = preprocessor(model)
        self.model = model
        self.query = query
        self.tables = tables
        self.table_data = table_data
        self.description = description if description else {}
        self.history = []
        self.steps = steps
        try:
            self.preprocess()
        except Exception as e:
            print(f"Error in preprocessing: {e}. Keep original tables.")
        if self.steps:
            self.parser = parser(model, query, self.tables)
        self.planner = None
        self.result = None

    def preprocess(self):
        if self.steps:
            result = self.preprocessor.preprocess_table(self.tables, self.query, self.steps)
            result = eval(result)
        else:
            result = self.preprocessor.preprocess_table(self.tables, self.query)
        for table_result in result:
            table_name = table_result['table_name']
            columns = table_result['columns']
            if type(columns) == str:
                columns = columns[1:-1].split(",")
                columns = [column.strip() for column in columns]
            try:
                self.table_data[table_name] = self.table_data[table_name][columns]
            except KeyError:
                for i in range(len(columns)):
                    if columns[i] == 'CreationDate':
                        columns[i] = 'CreaionDate'
                self.table_data[table_name] = self.table_data[table_name][columns]
        self.tables = get_tables_info(self.table_data, self.description)

    def parse(self):
        link = self.parser.link()
        decomposition = self.parser.decompose(link)
        if decomposition.startswith('```json'):
            decomposition = decomposition.split('```json')[1].split('```')[0].strip()
        try:
            decomposition = eval(decomposition)
        except:
            pass

        return link, decomposition


    def plan(self, n):
        res_content = self.planner.plan()
        if res_content.find("Action:") != -1:
            try:
                thought = str(res_content.split("Thought:")[1].split("Action:")[0]).strip()
            except:
                thought = 'None'
            tool_call = str(res_content.split("Action:")[1].split("Action Input:")[0]).strip()
            tool_call_arguments = str(res_content.split("Action Input:")[1].split("Observation:")[0]).strip()
            tool_call = tool_call.translate(str.maketrans('', '', '"\'')).strip()
            if tool_call.startswith('value_operator.') or tool_call.startswith('semantic_operator.'):
                tool_call = tool_call.split('.')[1]
            tool_call_arguments = tool_call_arguments.translate(str.maketrans('', '', '"\'')).strip()
            tool_call_arguments = [arg.strip() for arg in tool_call_arguments.split(";;")]
            if tool_call == 'value_calculate':
                table_name = '*'
            else:
                table_name = tool_call_arguments[0]
                tool_call_arguments = tool_call_arguments[1:]

            table_name, tool_call, tool_call_arguments, res_chain, cost, has_failed = self.execute(table_name, tool_call, tool_call_arguments)

            if tool_call == 'value_calculate':
                action_input = ';;'.join(tool_call_arguments)
                
            else:
                action_input = table_name + ';;' + ';;'.join(tool_call_arguments)
            
            if self.steps:
                if not thought.startswith('('):
                    self.planner.prompt += f"\n({n - 1}/{len(self.steps)}) Thought: {thought}\nAction: {tool_call}\nAction Input: {action_input}\nObservation: {res_chain}\nCurrent Tables: {str([name for name in self.table_data.keys()])}"
                else:
                    self.planner.prompt += f"\nThought: {thought}\nAction: {tool_call}\nAction Input: {action_input}\nObservation: {res_chain}\nCurrent Tables: {str([name for name in self.table_data.keys()])}"
            else:
                self.planner.prompt += f"\nThought: {thought}\nAction: {tool_call}\nAction Input: {action_input}\nObservation: {res_chain}\nCurrent Tables: {str([name for name in self.table_data.keys()])}\n"

            if self.steps and 'Failed to execute. Error message:' in res_chain:
                raise Exception(f"Execution failed: {res_chain}")
            
            self.history.append({
                'thought': thought,
                'action': tool_call,
                'action_input': action_input,
                'observation': res_chain,
                'current_tables': [name for name in self.table_data.keys()],
                'cost': cost,
                'has_failed': has_failed
            })

        else:
            self.result = res_content
            try:
                thought = str(res_content.split("Thought:")[1].split("Final Answer:")[0]).strip()
                final_answer = str(res_content.split("Final Answer:")[1]).strip()
                self.history.append({
                    'thought': thought,
                    'final answer': final_answer
                })
            except:
                self.history.append({
                    'final answer': res_content
                })

    def execute(self, table_name, tool_call, tool_call_arguments):
        self.executor = executor(table_name, tool_call, tool_call_arguments, self.query)
        lm = lotus.settings.lm
        cost_start = {
            'total_cost': lm.stats.total_usage.total_cost,
            'prompt_tokens': lm.stats.total_usage.prompt_tokens,
            'completion_tokens': lm.stats.total_usage.completion_tokens,
            'total_tokens': lm.stats.total_usage.total_tokens,
            'cache_hits': lm.stats.total_usage.cache_hits,
        }
        table_name, tool_call, tool_call_arguments, res_chain, self.table_data, has_failed = self.executor.execute(self.table_data)
        cost = {
            'total_cost': lm.stats.total_usage.total_cost - cost_start['total_cost'],
            'prompt_tokens': lm.stats.total_usage.prompt_tokens - cost_start['prompt_tokens'],
            'completion_tokens': lm.stats.total_usage.completion_tokens - cost_start['completion_tokens'],
            'total_tokens': lm.stats.total_usage.total_tokens - cost_start['total_tokens'],
            'cache_hits': lm.stats.total_usage.cache_hits - cost_start['cache_hits'],
        }
        return table_name, tool_call, tool_call_arguments, res_chain, cost, has_failed

    def solve(self):
        n = 1
        if self.steps:
            self.planner = planner_cost(self.query, self.tables, self.model, steps=self.steps)
        else:
            link, decomposition = self.parse()
            self.planner = planner(self.query, self.tables, self.model, link, decomposition)

        while not self.result:
            if self.steps and n > len(self.steps) + 1:
                break
            if not self.steps and n > 15:
                break
            print(f"Round {n}")
            self.plan(n)
            n += 1
        return self.history, self.result, self.table_data
