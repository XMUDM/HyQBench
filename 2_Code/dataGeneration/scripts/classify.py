tool = {
    'Filter': ['filter', 'sem_filter', 'continue'],
    'Ori_Filter':['filter'],
    'Sem_Filter':['sem_filter'],

    'Join': ['join'],
    'Sem_Join': ['sem_join'],

    'Map': ['map','sem_map'],
    'Sem_Map':['sem_map'],

    'Group': ['groupby'],
    'Sem_Group':['sem_cluster_by'],

    'Aggregate': ['agg', 'sem_agg'],
    'Sem_Aggregate': ['sem_agg'],

    'Value_Sort': ['orderby'],
    'Semantic_Sort': ['sem_topk'],
    'Head': ['head'],
}

query = {
    "Selection": [
        ["Join", "Sem_Filter", "Filter", "Filter", "Aggregate"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Value_Sort", "Head",  "Aggregate"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Value_Sort", "Head",  "Map"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Semantic_Sort", "Map"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Semantic_Sort", "Aggregate"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Group", "Filter", "Map"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Group", "Filter", "Sem_Aggregate"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Group", "Filter", "Value_Sort", "Head", "Map"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Group", "Filter", "Value_Sort", "Head", "Aggregate"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Group", "Filter", "Semantic_Sort", "Map"],
        ["Join", "Sem_Filter", "Filter", "Filter", "Group", "Filter", "Semantic_Sort", "Aggregate"],
    ],
    "Ranking": [
        ["Join", "Filter", "Semantic_Sort", "Aggregate"],
        ["Join", "Filter", "Semantic_Sort", "Map"],
        ["Join", "Filter", "Group", "Filter", "Semantic_Sort", "Map"],
        ["Join", "Filter", "Group", "Filter", "Semantic_Sort", "Aggregate"],
    ],
    "Group and Categorization": [
        ["Join", "Filter", "Sem_Group","Filter", "Map"],
        ["Join", "Filter", "Sem_Group","Filter","Aggregate"],
        ["Join", "Filter", "Sem_Group","Filter","Value_Sort", "Head",  "Map"],
        ["Join", "Filter", "Sem_Group","Filter","Value_Sort", "Head", "Aggregate"],
        ["Join", "Filter", "Sem_Group","Filter","Semantic_Sort","Map"],
        ["Join", "Filter", "Sem_Group","Filter","Semantic_Sort","Aggregate"],
    ],
    "Transformation": [
        ["Join", "Filter", "Semantic_Sort", 'Sem_Map'],
        ["Join", "Filter", "Value_Sort", "Head",  'Sem_Map'],
    ],
    "Linking": [
        ["Sem_Join", "Filter", "Aggregate"],
        ["Sem_Join", "Filter", "Value_Sort", "Head",  "Map"],
        ["Sem_Join", "Filter", "Value_Sort", "Head",  "Aggregate"],
        ["Sem_Join", "Filter", "Semantic_Sort", "Map"],
        ["Sem_Join", "Filter", "Semantic_Sort", "Aggregate"],
        ["Sem_Join", "Filter", "Group", "Filter", "Map"],
        ["Sem_Join", "Filter", "Group", "Filter", "Aggregate"],
        ["Sem_Join", "Filter", "Group", "Filter", "Value_Sort", "Head",  "Map"],
        ["Sem_Join", "Filter", "Group", "Filter", "Value_Sort", "Head",  "Aggregate"],
        ["Sem_Join", "Filter", "Group", "Filter", "Semantic_Sort", "Map"],
        ["Sem_Join", "Filter", "Group", "Filter", "Semantic_Sort","Aggregate"],
    ],
    "Summarization and Abstraction": [
        ["Join", "Filter", 'Sem_Aggregate'],
        ["Join", "Filter", "Value_Sort", "Head",  'Sem_Aggregate'],
        ["Join", "Filter", "Semantic_Sort", 'Sem_Aggregate'],
    ],
    "Explanation and Reasoning": [
        ["Join", "Filter", 'Sem_Aggregate'],
        ["Join", "Filter", "Value_Sort", "Head",  'Sem_Aggregate'],
        ["Join", "Filter", "Semantic_Sort", 'Sem_Aggregate'],
    ],
    "Prediction and Planning": [
        ["Join", "Filter", 'Sem_Aggregate'],
        ["Join", "Filter", "Value_Sort", "Head",  'Sem_Aggregate'],
        ["Join", "Filter", "Semantic_Sort", 'Sem_Aggregate'],
    ]
}
