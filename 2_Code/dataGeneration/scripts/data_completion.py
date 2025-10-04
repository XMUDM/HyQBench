import json
import re
from .tool_pool import tools, tools_simplified
import sys
import sys
sys.path.append('../..')
from utils.llm import DP

# add query for a complete operator call process
def add_query(process):
    model = DP()
    prompt = f"""You are a database expert who can reversely summarize its corresponding database natural language query for a complete operator call process.
### The functions of possible operators are described as follows:
{json.dumps(tools_simplified, indent = 4)}
Among them, semantic operators starting with "sem_" are used for operations when some common sense information is missing or semantic understanding and reasoning are required to obtain results.

### The following points need to be paid attention to:
1. The operator calling process should be strictly consistent with natural language query, and there should be no ambiguity.
2. The table name should not be specified directly.
3. For sem_join, please specify the specific conditions for the join.
4. No key information is lost, such as the number of intercepted rows, etc.

### Example 1:
Process:
[
    {{
        "initial_table": [
            {{
                "loan": {{
                    "*": "all columns",
                    "loan_id": "description: column_description=the id number identifying the loan data; value_description=NULL dtype=int64 e.g. 5314; 5625; 6229; 7308; 6342; 6399",
                    "account_id": "description: column_description=the id number identifying the account; value_description=NULL dtype=int64 e.g. 3084; 309; 132; 5541; 9928; 5700",
                    "date": "description: column_description=the date when the loan is approved; value_description=NULL dtype=object e.g. 1995-01-02; 1998-01-27; 1994-02-27; 1996-09-09; 1998-12-02; 1997-12-06",
                    "amount": "description: column_description=approved amount; value_description=unit US dollar dtype=int64 e.g. 61656; 84600; 5148; 299088; 76380; 103680",
                    "duration": "description: column_description=loan duration; value_description=unit month dtype=int64 e.g. 48; 24; 60; 12; 36",
                    "payments": "description: column_description=monthly payments; value_description=unit amonth dtype=float64 e.g. 7904.0; 2718.0; 4108.0; 6365.0; 7042.0; 2077.0",
                    "status": "description: column_description=repayment status; value_description='A' stands for contract finished, no problems; 'B' stands for contract finished, loan not paid; 'C' stands for running contract, OK so far; 'D' stands for running contract, client in debt dtype=object e.g. C; A; D; B"
                }}
            }},
            {{
                "account": {{
                    "*": "all columns",
                    "account_id": "description: column_description=the id of the account; value_description=NULL dtype=int64 e.g. 6738; 1766; 5944; 7769; 10280; 2361",
                    "district_id": "description: column_description=location of branch; value_description=NULL dtype=int64 e.g. 29; 49; 13; 9; 10; 45",
                    "frequency": "description: column_description=frequency of the acount; value_description=NULL dtype=object e.g. POPLATEK MESICNE; POPLATEK PO OBRATU; POPLATEK TYDNE",
                    "date": "description: column_description=the creation date of the account; value_description=in the form YYMMDD dtype=object e.g. 1997-08-18; 1996-03-21; 1997-03-08; 1993-10-25; 1997-06-06; 1995-01-19"
                }}
            }}
        ]
    }},
    {{
        "operator": "filter",
        "params": "loan;;duration;;[(ge, 24), (le, 36)]",
        "result": "loan_filter",
        "current_table": [
            "loan",
            "account",
            "loan_filter"
        ]
    }},
    {{
        "operator": "sem_join",
        "params": "loan_filter;;account;;{{frequency:right}} is 'POPLATEK MESICNE' and {{status:left}} is 'A'",
        "result": "loan_filter_sem_join",
        "current_table": [
            "loan",
            "account",
            "loan_filter",
            "loan_filter_sem_join"
        ]
    }},
    {{
        "operator": "orderby",
        "params": "loan_filter_sem_join;;[duration, payments];;asc",
        "result": "loan_filter_sem_join_orderby",
        "current_table": [
            "loan",
            "account",
            "loan_filter",
            "loan_filter_sem_join",
            "loan_filter_sem_join_orderby"
        ]
    }},
    {{
        "operator": "head",
        "params": "loan_filter_sem_join_orderby;;20",
        "result": "loan_filter_sem_join_orderby_head",
        "current_table": [
            "loan",
            "account",
            "loan_filter",
            "loan_filter_sem_join",
            "loan_filter_sem_join_orderby",
            "loan_filter_sem_join_orderby_head"
        ]
    }},
    {{
        "operator": "map",
        "params": "loan_filter_sem_join_orderby_head;;[amount, payments, frequency, district_id]",
        "result": "loan_filter_sem_join_orderby_head_map",
        "current_table": [
            "loan",
            "account",
            "loan_filter",
            "loan_filter_sem_join",
            "loan_filter_sem_join_orderby",
            "loan_filter_sem_join_orderby_head",
            "loan_filter_sem_join_orderby_head_map"
        ]
    }}
]

Response:
For loans with durations between 24 and 36 months, join tables with "{{frequency}} is 'POPLATEK MESICNE' and {{status}} is 'A'" as the condition, and then show the amount, payments, frequency, and district_id of the first 20 results with minimum duration and payments.

### Example 2:
Process:
[
    {{
        "initial_table": [
            {{
                "satscores": {{
                    "AvgScrMath": "e.g. 561.0; 446.0; 436.0; 521.0; 422.0; 462.0",
                    "cname": "e.g. Butte; Alameda; Butte; Alameda; Amador; Alameda"
                }}
            }}
        ]
    }},
    {{
        "Action": "filter",
        "Action Input": "satscores;;AvgScrMath;;gt;;560",
        "Observation": "result: {{'AvgScrMath': 'e.g. 567.0; 580.0; 611.0; 561.0; 629.0; 599.0', 'cname': 'e.g. Alameda; Alameda; Alameda; Alameda; Alameda; Alameda'}}"
        "Current Table": ['satscores', 'satscores_filter']
    }},
    {{
        "Action": "sem_filter",
        "Action Input": "satscores_filter;;{{cname}} is in the bay area",
        "Observation": "result: {{'AvgScrMath': 'e.g. 565.0; 629.0; 599.0; 622.0; 699.0; 561.0', 'cname': 'e.g. Alameda; Alameda; Alameda; Alameda; Alameda; Alameda'}}",
        "Current Table": ['satscores', 'satscores_filter', 'satscores_filter_sem_filter']
    }},
    {{
        "Action": "agg",
        "Action Input": "satscores_filter_sem_filter;;cname;;count",
        "Observation": "result: 23",
        "Current Table": ['satscores', 'satscores_filter', 'satscores_filter_sem_filter']
    }}
]

Response:
Among the schools with an average score in Math over 560 in the SAT test, how many schools are located in the bay area?

### The process that needs to be summarized is as follows:
{json.dumps(process, indent = 4)}

### Response(Please generate a natural language query corresponding to this process. Replies should include only queries and not other content.):
"""

    print(prompt)
    response = model(prompt)
    if response.startswith('\"') and response.endswith('\"'):
        response = response[1:-1]
    return response

# add thought for each step in the process
def add_thought(query, process):
    print(json.dumps(process, indent=4))
    model = DP()
    prompt = f"""
You are a database query reasoning expert. Your task is to supplement the thinking process and final answer for the given query and corresponding correct answer process.

The operators are divided into two categories: value_operator and semantic_operator. Operators include value_operator and semantic_operator. value_operator is used for operations that can directly get results from the table, while semantic_operators starting with 'sem_' are used for operations that require combining external knowledge. For example, when the table lacks common sense information such as location or gender, making all columns unable to answer, or when semantic reasoning is needed to get the result.

All operators are as follows:
{json.dumps(tools, indent=4)}

### Example:
Query: "Among the schools with the average score in Math over 560 in the SAT test, how many schools are in the bay area?"

Answer Process:
[
    {{
        "initial_table": [
            {{
                "satscores": {{
                    "AvgScrMath": "e.g. 561.0; 446.0; 436.0; 521.0; 422.0; 462.0",
                    "cname": "e.g. Butte; Alameda; Butte; Alameda; Amador; Alameda"
                }}
            }}
        ]
    }},
    {{
        "Action": "filter",
        "Action Input": "satscores;;AvgScrMath;;gt;;560",
        "Observation": "result: {{'AvgScrMath': 'e.g. 567.0; 580.0; 611.0; 561.0; 629.0; 599.0', 'cname': 'e.g. Alameda; Alameda; Alameda; Alameda; Alameda; Alameda'}}"
        "Current Table": ['satscores', 'satscores_filter']
    }},
    {{
        "Action": "sem_filter",
        "Action Input": "satscores_filter;;{{cname}} is in the bay area",
        "Observation": "result: {{'AvgScrMath': 'e.g. 565.0; 629.0; 599.0; 622.0; 699.0; 561.0', 'cname': 'e.g. Alameda; Alameda; Alameda; Alameda; Alameda; Alameda'}}",
        "Current Table": ['satscores', 'satscores_filter', 'satscores_filter_sem_filter']
    }},
    {{
        "Action": "agg",
        "Action Input": "satscores_filter_sem_filter;;cname;;count",
        "Observation": "result: 23",
        "Current Table": ['satscores', 'satscores_filter', 'satscores_filter_sem_filter']
    }}
]

Response:
[
    {{
        "Thought": "I need to filter the schools with an average score in Math over 560 from the table `satscores`. Then, I need to filter the resulting schools to find those in the bay area. Finally, I will count the number of schools that meet both criteria.",
    }},
    {{
        "Thought": "Now that I have filtered the schools with an average score in Math over 560, I need to filter these schools to find those in the bay area. Since the bay area is a semantic concept, I will use the `sem_filter` tool to identify schools in the bay area.",
    }},
    {{
        "Thought": "Now that I have filtered the schools in the bay area, I need to count the number of schools that meet both criteria (average score in Math over 560 and located in the bay area). I will use the `agg` tool to count the number of schools.",
    }},
    {{
        "Thought": "I now know the final answer",
        "Final Answer": 23
    }}
]

### Your task: 
Query: {query}

Answer Process:
{json.dumps(process, indent=4)}

Response(starting with '['):
    """
    print(prompt)
    response = model(prompt)
    print(response)
    if '```json' in response:
        response = response.replace('```json', '').replace('```', '').strip()
    try:
        response = eval(response)
    except:
        thoughts = re.findall(r'"Thought":\s*"([^"]*)"', response)
        final_answer_match = re.search(r'"Final Answer":\s*(\{[\s\S]*?\})\s*\]\s*$', response)
        final_answer = final_answer_match.group(1) if final_answer_match else None
        results = []
        assert len(thoughts)  == len(process), "The number of thoughts must match the number of steps in the process minus one."
        for thought in thoughts[:-1]:
            results.append({"Thought": thought})
        results.append({"Thought": thoughts[-1], "Final Answer": final_answer})
        response = results
    return response
