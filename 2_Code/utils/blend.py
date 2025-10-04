import json
from blendsql import BlendSQL
from blendsql.models import LiteLLM

def blend(sql_model, query, table_data, table_info):
    model = LiteLLM("openai/gpt-4o-mini")

    # Prepare our BlendSQL connection
    bsql = BlendSQL(
        table_data,
        model=model,
        verbose=True,
    )

    blend_prompt = """You are a helpful assistant that can transform natural language queries into BlendSQL queries. BlendSQL is a superset of SQLite, which adds LLM operators: LLMQA, LLMMap, LLMJoin to the SQL syntax. Here are some examples.

# Examples 1
## Query: Summarize the characteristics of countries and their capitals in North America.

## Table:
Country: {{
    "name": "USA", "Canada", "Mexico", "France",
    "capital": "Washington D.C.", "Ottawa", "Mexico City", "Paris",
    "continent": "North America", "North America", "North America", "Europe"
}}

## BlendSQL Query:
SELECT {{
    LLMQA(
        'Summarize the characteristics of North American countries.',
        (SELECT name FROM Country WHERE continent='North America')
    )
}} as Answer

# Examples 2
## Query: How many countries are located in Asia?

## Table:
Country: {{
    "name": "USA", "China", "India", "France",
    "capital": "Washington D.C.", "Beijing", "New Delhi", "Paris
}}

## BlendSQL Query:
SELECT COUNT(T1.name) FROM Country AS T1 WHERE {{ LLMMap( 'Is the country located in Asia?' T1.name ) }} = TRUE

# Examples 3
## Query: Join the Country and City tables to find all countries along with their capitals.

## Tables:
Country: {{
    "name": "USA", "Canada", "Mexico", "France", "Germany"
}}

City: {{
    "name": "New York", "Washington D.C.", "Toronto", "Vancouver", "Paris", "Berlin"
}}

## BlendSQL Query:
SELECT * FROM Country
JOIN City on {{
    LLMJoin(
        join_criteria="City.name is the capital of Country.name",
        Country.name,
        City.name
    )
}}

Now, given the following natural language query, convert it into a BlendSQL query:
## Query: """ + query + """

## Tables:
""" + json.dumps(table_info, indent=4) + """

## BlendSQL Query: (only provide the SQL query, do not include any explanations)
"""

    # Execute the BlendSQL query
    sql = sql_model(blend_prompt)
    if sql.startswith("```sql"):
        sql = sql[7:-3].strip()
    print("Generated BlendSQL Query:\n", sql)
    result = bsql.execute(sql)

    print(result.df)

    print('Time (s):', result.meta.process_time_seconds)
    print('# Generation Calls:', result.meta.num_generation_calls)
    print('Prompt Tokens:', result.meta.prompt_tokens)
    print('Completion Tokens:', result.meta.completion_tokens)

    return sql, result