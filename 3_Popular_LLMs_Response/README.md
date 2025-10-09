<div align='center'>
    <h1>Popular LLMs' Responses</h1>
</div>

## 📑 Contents

* [Overview](#📝-overview)
* [Download Link](#🔗-download-link)
* [Data Composition](#📋-data-composition)
* [Data Example](#📊-data-example)

## 📝 Overview

This section shows the response of multiple popular large language models on HyQBench. By testing this response dataset, we can comprehensively evaluate the performance of different LLMs and methods in hybrid queried. This directory contains model response examples and download links for the complete response dataset. It is divided into three sub-directories: end2end-lotus, end2end-blendsql, and cost.

## 🔗 Download Link

This repository only displays part of the data. For the complete dataset, please go to [HyQBench/Popular_LLMs_Response.tar.gz](https://drive.google.com/drive/folders/1VpyF05rvHxEAIU3bwERNnWosrpqK6rri?usp=sharing) to download.

## 📋 Data Composition

|       Model        | End-to-End-lotus | End-to-End-blendsql | Cost |                            Link                            |
| :----------------: | :---------------: | :--------------: | :---------------: | :--------------------------------------------------------: |
|      GPT-4.1.      |         √         |        √         |         √         |                 https://openai.com/                  |
|    DeepSeek-V3     |         √         |        √         |         √         |                    https://huggingface.co/deepseek-ai/DeepSeek-V3-0324          |
|  Gemini-2.5-Flash  |         √         |        √         |         √         |                    https://deepmind.google/models/gemini/pro/                   |
|     Qwen-Max       |         √         |        √         |         √         |   https://qwen.ai/    |
|      Qwen-8B       |         √         |        √         |         √         |   https://huggingface.co/Qwen/Qwen3-8B |
|    Qwen-8B-sft     |         √         |        ×         |         √         |     \     |
|    Qwen-4B-sft     |         √         |        ×         |         √         |     \     |


## 📊 Data Example

### LOTUS

```json
{
    "query": "How many races occurred in October of each year, ordered by year in ascending order?",
    "history": [
        {
            "thought": "To answer this, I need to extract the month from the races.date column and filter for October (month = 10). Since the month is embedded in the date string, I need to use a semantic operator to extract the month. Then, I will filter for month = 10, group by year, count the number of races per year, and finally order by year ascending. I will use sem_map to extract the month, then filter, groupby, and orderby.",
            "action": "sem_map",
            "action_input": "races;;Extract the month as a two-digit number from {date}. Respond with only the month number.",
            "observation": "races_sem_map: {'*': 'all columns', 'raceId': 'dtype=int64 e.g. 454; 20; 365; 14; 937; 679', 'circuitId': 'dtype=int64 e.g. 37; 6; 34; 61; 21; 9', 'year': 'dtype=int64 e.g. 1972; 1991; 2002; 1974; 1965; 1966', 'date': 'dtype=object e.g. 2013-09-08; 2003-06-01; 2008-08-03; 1958-05-26; 1969-10-19; 1968-09-22', '_map': 'dtype=object e.g. 10; 12; 11; 01; 04; 06'} table_length: 954",
            "current_tables": [
                "races",
                "races_sem_map"
            ],
            "cost": {
                "total_cost": 0.01087559999999499,
                "prompt_tokens": 64872,
                "completion_tokens": 1908,
                "total_tokens": 66780,
                "cache_hits": 0
            },
            "has_failed": false
        },
        ...
        {
            "thought": "I now know the final answer",
            "final answer": "races_sem_map_filter_groupby_orderby"
        }
    ],
    "result": "Thought: I now know the final answer\nFinal Answer: races_sem_map_filter_groupby_orderby",
    "link": "Let's think step by step.\n\n**Step 1: Understand the question.**  ...",
    "decomposition": [
        "extract the month from races.date and filter rows where the month is 10 (October) (value)",
        "group the filtered rows in #1 by races.year (value)",
        "count the number of races (e.g., raceId) in each group in #2 (value)",
        "order the results of #3 by races.year in ascending order (value)"
    ]
}
```


### BlendSQL

```json
{
    "query": "Show the molecule IDs and their carcinogenic labels for all molecules that contain nitrogen atoms.",
    "SQL": "SELECT DISTINCT m.molecule_id, m.label\nFROM molecule AS m\nJOIN atom AS a ON m.molecule_id = a.molecule_id\nWHERE {{ LLMMap('Does the atom element represent nitrogen?', a.element) }} = TRUE",
    "cost": {
        "prompt_tokens": 486,
        "completion_tokens": 18,
        "total_tokens": 504
    }
}
```
