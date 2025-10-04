import json
import sys
sys.path.append('..')
from dataGeneration.scripts.tool_pool import tools, tool_names

class planner:
    def __init__(self, query, tables, model, link=None, decomposition=None):
        self.query = query
        self.tables = tables
        self.decomposition = decomposition
        self.link = link
        self.model = model
        self.prompt = self.init_prompt()
    
    def init_prompt(self):
        prompt = f"""
You are a database query planner, which requires you to formulate a query plan based on the given query and the provided data information and operators. The values in tables are example values. The operators include value_operator and semantic_operator. The value_operator is used for operations that can directly obtain results from the table, while the semantic_operator is used for operations that require combined external knowledge, such as when common-sense information like location or gender is missing, or when semantic understanding and reasoning are needed to obtain the result. The input of semantic operators usually includes a prompt containing column names to replace specific data. The following conditions should be met when formulating the query plan:
1. The input of operators includes table name and arguments, separated by ";;".
2. Provide the final result directly, without the need for further operations, and the final result must be achieved through the call operator.
3. If there is an error, you need to reflect on whether the tools you have called and the input format are correct.

The table and corresponding column information used are as follows:
{json.dumps(self.tables, indent=4)}

Answer the following questions as best you can. The input of tools are separated by ";;". You have access to the following tools and the example input within the double quotes:
{json.dumps(tools, indent=4)}

Use the following format and your output will be matched exactly by the code, so do not include any additional symbols, just the following text is needed:

Question: the input question you must answer
Thought: I will check the thought and result of the previous step to see if it has been solved correctly and completely ... I will select the ... tool because ... I will input ... into the tool because ...
Action: the action to take, should be one of {tool_names}
Action Input: the input to the action
Observation: the result of the action (If it is a tables, do not use the example values directly as criteria for semantic operations. Empty table means no there are no items that meet the criteria.)
Current Tables: list of current existing tables
... (this Thought/Action/Action Input/Observation/Current Tables can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {self.query}
"""
        if self.link:
            prompt += f"\nSchema Link:\n{self.link}"
        if self.decomposition:
            prompt += f'\nYou can refer to the following steps.\n{json.dumps(self.decomposition, indent=4)}'
        return prompt

    def plan(self):
        res_content = self.model(self.prompt)
        return res_content
