import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import lotus
from lotus.sem_ops_batch import sem_filter

def callTool_batch(*argv):
    table_data = argv[0]
    if argv[1] != '*' and argv[1] not in table_data:
        return f"Invalid: Table {argv[1]} not found."
    df = table_data[argv[1]] if argv[1] != '*' else None
    ops = argv[2]
    argus = argv[3]

    try:
        if ops == 'sem_map' and len(argus) == 2:
            processed_df = dedup(df, argus[0])
            mapped_df = processed_df.sem_map(argus[0])
            return postprocess_sem_map(df, mapped_df, argus[0])
        elif ops == 'sem_filter_batch' and len(argus) == 2:
            processed_df = dedup(df, argus[0])
            filtered_df =  processed_df.sem_filter_batch(user_instruction=argus[0], batch_size=argus[1])
            return postprocess_sem_filter(df, filtered_df)
        else:
            return "Invalid arguments."
    except Exception as e:
        import traceback
        traceback.print_exc()
        return "Invalid arguments. Error message: " + str(e)
    
def dedup(df, insturction):
    col_li = lotus.nl_expression.parse_cols(insturction)
    col_li = list(set(col_li))
    for column in col_li:
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in DataFrame")
    temp_df = df.copy()
    temp_df = temp_df[col_li]
    temp_df = temp_df.drop_duplicates()
    temp_df = temp_df.dropna()
    return temp_df

def postprocess_sem_filter(df, filtered_df):
    col_li = filtered_df.columns
    df = df.merge(filtered_df[col_li], on=list(col_li), how='inner')
    return df

def postprocess_sem_map(df, mapped_df, insturction):
    cols = lotus.nl_expression.parse_cols(insturction)
    df = df.merge(mapped_df, on=list(cols), how='inner')
    return df
