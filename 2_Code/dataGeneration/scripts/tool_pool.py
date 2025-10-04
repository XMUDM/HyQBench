tools = {
    'filter': 'Filter data. Input the column name, a list of filter condition tuples where the operator can only be one of eq, ne, gt, lt, ge, le, like, along with the value to be compared, and these conditions need to be satisfied simultaneously. e.g. Student;;age;;[(gt, 18), (lt, 20)]',
    'join': 'Connect two tables. Input the table name to be connected to this table and the corresponding column names of the two tables. e.g. Person;;Student;;SID;;stuID',
    'agg': 'Using an aggregate function. Input the column name and the function name, which must be one of sum, mean, count, min, max. e.g. Student;;year;;max',
    'head': 'Fetch the first n data entries. Input the number of entries to fetch. e.g. School;;10',
    'groupby': 'Group by the content of the specified column and use aggregate functions. Input a list of column names to group by, and an column name not in the groupby list on which the aggregate function is applied and the name of a aggregate function. e.g. Teacher;;[school, country];;year;;max',
    'orderby': 'Sort according to the specified column. Input a list of column names and specify the order, which can only be asc or desc. e.g. Student;;[age, name];;asc',
    'map': 'Project on the columns of the table. Input is a list of column names. e.g. Student;;[name, age]',
    'column_calculate': 'Perform calculations on the two columns of the table. Input the names of two columns and the operator, which can only be one of add, sub, mul, div. e.g. Doctor;;age;;year;;add',
    'value_calculate': 'Perform calculations on the values. Input the operator, which can only be one of add, sub, mul, div, round, and the actual numerical value to be calculated. e.g. add;;1;;2 or round;;11.23',
    'sem_filter': 'Input the table name and a statement containing column names that can be judged as true or false, and return the rows that are judged as true. e.g. Paper;;{Text} is about statistics',
    'sem_join': 'Connect two tables through semantics. Input the two table names and a statement containing the two column names of the two tables that can be judged as true or false, and return the connection result judged as true. e.g. Teacher;;Course;;Taking {Course Name:left} will help me learn {Skill:right}',
    'sem_map': "Maps a column to a new output column. Can be used to extract content or features from a column, or to convert the format of a column's content, etc. e.g. Posts;;Extract the statistical term from {Title}. Respond with only the statistical term.",
    'sem_agg': 'Given an instruction, perform an aggregation operation on the input tuple, such as summarizing, explaining, or judging, etc. e.g. Paper;;Summarize common characteristics of the {Title}s',
    'sem_topk': 'Ranks each tuple and returns the k best according to the prompt, which specifies a ranking function that sorts a list of tuples. Input a question containing a column name and specify the number of tuples to return. e.g. Team;;What {team_long_name} has the most fans?;;1',
    'sem_cluster_by': 'Automatically cluster based on semantics similarity, then provide a summary for each category and use an aggregation function for each category. Please input the table name, the column to cluster, the number of cluster centers, the column on which the aggregate function is applied and the name of a aggregate function. For example: Team;;team_description;;3;;score;;max',
}

tools_simplified = {
    'filter': 'Filter data using the specified column, operator, and value.',
    'join': 'Connect two tables using the specified column names.',
    'agg': 'Using an aggregate function on the specified column.',
    'head': 'Fetch the first n data entries.',
    'groupby': 'Group by the content of the specified column and use aggregate functions.',
    'orderby': 'Sort according to the specified column.',
    'map': 'Project on the columns of the table.',
    'column_calculate': 'Perform calculations on the two columns of the table.',
    'value_calculate': 'Perform calculations on the values.',
    'sem_filter': 'Input the table name and a statement containing column names that can be judged as true or false.',
    'sem_join': 'Connect two tables through semantics using a statement containing the column names of the two tables.',
    'sem_map': 'Maps a column to a new output column using a statement.',
    'sem_agg': 'Given an instruction, perform an aggregation operation on the input tuple.',
    'sem_topk': 'Ranks each tuple and returns the k best according to the prompt.',
    'sem_cluster_by': 'Automatically cluster based on semantics similarity, then provide a summary for each category and use an aggregation function for each category.'
}

tool_names = [
    'filter',
    'join',
    'agg',
    'head',
    'groupby',
    'orderby',
    'map',
    'column_calculate',
    'value_calculate',
    'sem_filter',
    'sem_join',
    'sem_agg',
    'sem_topk',
    'sem_map',
    'sem_cluster_by'
]
