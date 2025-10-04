import json
from .llm import DP

class parser:
    def __init__(self, model, query, tables):
        self.model = model
        self.query = query
        self.tables = tables
    
    def link(self):
        prompt = f"""
Find the schema_links for generating SQL queries for each question based on the database schema and Foreign keys.  

Two examples are as follows:

Table city, columns = [*,City_ID,Official_Name,Status,Area_km_2,Population,Census_Ranking] Table competition_record, columns = [*,Competition_ID,Farm_ID,Rank] Table farm, columns = [*,Farm_ID,Year,Total_Horses,Working_Horses, Total_Cattle,Oxen,Bulls,Cows,Pigs,Sheep_and_Goats] Table farm_competition, columns = [*,Competition_ID,Year,Theme,Host_city_ID,Hosts] Foreign_keys = [farm_competition.Host_city_ID = city.City_ID,competition_record.Farm_ID = farm.Farm_ID,competition_record.Competition_ID = farm_competition.Competition_ID] 
Q: "Show the status of the city that has hosted the greatest number of competitions." 
A: Let’s think step by step. In the question "Show the status of the city that has hosted the greatest number of competitions.", we are asked: "the status of the city" so we need column = [city.Status] "greatest number of competitions" so we need column = [farm_competition.*] Based on the columns and tables, we need these Foreign_keys = [farm_competition.Host_city_ID = city.City_ID]. Based on the tables, columns, and Foreign_keys, The set of possible cell values are = []. So the Schema_links are: Schema_links: [city.Status,farm_competition.Host_city_ID = city.City_ID,farm_competition.*]  

Table department, columns = [*,Department_ID,Name,Creation,Ranking,Budget_in_Billions,Num_Employees] Table head, columns = [*,head_ID,name,born_state,age] Table management, columns = [*,department_ID,head_ID,temporary_acting] Foreign_keys = [management.head_ID = head.head_ID,management.department_ID = department.Department_ID] 
Q: "How many heads of the departments are older than 56 ?" 
A: Let’s think step by step. In the question "How many heads of the departments are older than 56 ?", we are asked: "How many heads of the departments" so we need column = [head.*] "older" so we need column = [head.age] Based on the columns and tables, we need these Foreign_keys = []. Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [56]. So the Schema_links are: Schema_links: [head.*,head.age,56]

Now you need to generate the schema_links for the following Query based on the database schema. Some information cannot be directly obtained from the table and may require inference by combining columns in the table and external knowledge, please point out which columns need to be combined. Some connections between tables are not necessarily through primary keys and foreign keys, but could also be through the semantic relationship between two columns, please point out the two columns and their relationship.

{json.dumps(self.tables, indent=4, ensure_ascii=False)}
Q: "{self.query}" 
A: 
        """
        res_content = self.model(prompt)
        return res_content

    def decompose(self, schema):
        prompt = f"""
Given a query and the corresponding schema link, please provide the decomposition steps for the query. Some steps cannot be directly completed with the information in the table and may require inference by combining columns in the table and external knowledge. If external knowledge is needed, such as geographical location or gender information, please give "semantic". Otherwise, give "value".

Three examples of result are provided below:
### Examples:
query": "Among the posts owned by csgillespie, how many of them are root posts and mention academic papers?",
"result": [
    "filter the user named csgillespie in table users",
    "filter posts that are root posts in table posts"
    "filter posts mentioning academic papers in #2"
    "return the join result of #1 and #3",
    "return the count of #4"
]

"query": "How many Asian drivers competed in the 2008 Australian Grand Prix?",
"result": [
    "filter Asian drivers in table drivers",
    filter races held in 2008 in table races"
    "filter races named Australian Grand Prix in #2"
    "return the join result of #3 and table results"
    "return the join result of #1 and #4"
    "return the count of #5"
]

"query": "Of the top three away teams that scored the most goals, which one has the most fans?",
"result": [
    "return the join result of table Match and table Team"
    "return the top three away teams that scored the most goals in #1"
    "return the team with the most fans in #2"
]

The join operation must be explicitly indicated it as in the example. Your output should start from "[" and do not add any other content.

### Query:
{self.query}

### Tables:
{json.dumps(self.tables, indent=4, ensure_ascii=False)}

### Schema:
{schema}

result:
        """
        res_content = self.model(prompt)
        return res_content
