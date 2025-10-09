<div align='center'>
    <h1>Dataset</h1>
</div>

## 📑 Contents

* [Overview](#-overview)
* [Download Link](#-download-link)
* [Directory Structure](#-directory-structure)
* [Data Example](#-data-example)


## 📝 Overview

This section shows the HyQBench dataset, which is constructed by an innovative method based on large language model generation. The dataset contains 7,463 queries, covering six query types, with each query consisting of three parts: natural language query, reasoning process, and answer. This directory provides data examples and download links for the complete dataset.

## 🔗 Download Link

This repository only displays part of the data. For the complete dataset, please go to [HyQBench/Dataset.tar.gz](https://drive.google.com/drive/folders/1VpyF05rvHxEAIU3bwERNnWosrpqK6rri?usp=sharing) to download.


## 📂 Directory Structure
```
Dataset
├── aggregate
│   ├── query
│   └── table
├── filter
├── group
├── join
├── sort
└── transform
```

A total of six types, each type has two folders, `query` and `table`, which store the json files for queries and the corresponding intermediate result tables, respectively.

## 📊 Data Example

```json
{
    "database": "student_club",
    "type": "flat",
    "query": "How many members are associated with majors related to the Technology industry?",
    "process": [
        {
            "initial_table": [
                {
                    "member": "description of member"
                },
                {
                    "major": "description of major"
                }
            ]
        },
        {
            "Thought": "First, I need to join the 'member' and 'major' tables using the 'link_to_major' and 'major_id' columns to get information about which majors each member is associated with.",
            "operator": "join",
            "params": "member;;major;;link_to_major;;major_id",
            "result": "member_join: {...} table_length: 32",
            "current_table": [
                "member",
                "major",
                "member_join"
            ],
            "execution_time": 0.012832403182983398,
        },
        {
            "Thought": "Now that I have the joined table, I need to filter for majors that are related to the Technology industry. Since this requires semantic understanding of what constitutes a 'Technology' major, I'll use the 'sem_filter' operator.",
            "operator": "sem_filter",
            "params": "member_join;;{major_name} is associated with the Technology industry",
            "result": "member_join_sem_filter: {...} table_length: 7",
            "current_table": [
                "member",
                "major",
                "member_join",
                "member_join_sem_filter"
            ],
            "execution_time": 80.08473992347717
        },
        {
            "Thought": "With the filtered table containing only members associated with Technology-related majors, I now need to count how many members are in this filtered table. I'll use the 'agg' operator with the count function on the 'member_id' column.",
            "operator": "agg",
            "params": "member_join_sem_filter;;member_id;;count",
            "result": "7",
            "current_table": [
                "member",
                "major",
                "member_join",
                "member_join_sem_filter"
            ],
            "execution_time": 0.0002243518829345703
        },
        {
            "Thought": "I now know the final answer",
            "Final Answer": 7
        }
    ]
}
