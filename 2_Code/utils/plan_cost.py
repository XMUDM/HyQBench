import json
import sys
sys.path.append('..')
from dataGeneration.scripts.tool_pool import tools, tool_names

class planner_cost:
    def __init__(self, query, tables, model, steps=None):
        self.query = query
        self.tables = tables
        self.steps = steps
        self.model = model
        self.prompt = self.init_prompt()

    def init_prompt(self):
        prompt = f"""You are a database query planner, which requires you to arrange the order of actions to formulate a query plan based on the given query and the provided data information and operators. The following conditions should be met when formulating the query plan:
1. The input of operators includes table name and arguments, separated by ";;".
2. All the actions should be chosen and only chosen for one time.
3. The key_params should be the same as provided and the tables operated on should be added into parameters by yourself.

The table and corresponding column information used are as follows:
{json.dumps(self.tables, indent=4)}

Answer the following questions as best you can. The input of tools are separated by ";;". The all tools may be used are as follows:
{json.dumps(tools, indent=4)}

I provide you with the correct action and the corresponding key parameters. Semantic operators that start with "sem_" will call the large language model once for each piece of data. Please select and call them in a reasonable order, so that the answering process is correct and the number of times the large language model is called is minimal. The {len(self.steps)} steps you need to choose are as follows:
{json.dumps(self.steps, indent=4)}

Use the following format and your output will be matched exactly by the code, so do not include any additional symbols, just the following text is needed:

Question: the input question you must answer
Thought: the thought process you have, which should be a brief description of what you are thinking about
Action: the action to take, should be one of {tool_names}
Action Input: the input to the action
Observation: the result of the action
Current Tables: list of current existing tables
... (this Thought/Action/Action Input/Observation/Current Tables can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {self.query}"""

        return prompt

    def plan(self):
        res_content = self.model(self.prompt)
        return res_content
