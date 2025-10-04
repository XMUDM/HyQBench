import os
import sys
import pandas as pd
from .tool_pool import tool_names
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import lotus
from lotus.sem_ops import sem_map, sem_filter, sem_agg, sem_join, sem_topk, sem_cluster_by
from lotus.val_ops import filter, orderby, head, map, join, agg, groupby, column_calculate, value_calculate

def callTool(*argv):
    table_data = argv[0]
    if argv[1] != '*' and argv[1] not in table_data:
        return f"Invalid: Table {argv[1]} not found."
    df = table_data[argv[1]] if argv[1] != '*' else None
    ops = argv[2]
    argus = argv[3]
    
    if ops == 'sem_topk' or ops == 'sem_join':
        if ops == 'sem_topk':
            instruction = argus[0]
        else:
            instruction = argus[-1]
        if ops == 'sem_join':
            try:
                other_df = table_data[argus[0]]
            except KeyError:
                return f"Invalid: Table {argus[0]} not found."
        columns = lotus.nl_expression.parse_cols(instruction)
        try:
            for column in columns:
                if ops == 'sem_join' and 'left' in column:
                    column = column.split(':')[0]
                    if df[column].dtype != 'object':
                        return f"Invalid arguments: Numeric column should not be applied to semantic operators"
                if ops == 'sem_join' and 'right' in column:
                    column = column.split(':')[0]
                    if other_df[column].dtype != 'object':
                        return f"Invalid arguments: Numeric column should not be applied to semantic operators"
                else:
                    if df[column].dtype != 'object':
                        return f"Invalid arguments: Numeric column should not be applied to semantic operators"
        except KeyError as e:
            return f"Invalid arguments: Column {str(e)} not found in DataFrame"

    if ops not in tool_names:
        return "Invalid: Operation not in the supported list"
    try:
        if ops == 'filter' and len(argus) == 2:
            return df.Filter(argus[0], argus[1])
        elif ops == 'orderby' and len(argus) == 2:
            return df.Orderby(argus[0], argus[1])
        elif ops == 'head' and len(argus) == 1:
            return df.Head(argus[0])
        elif ops == 'map' and len(argus) == 1:
            return df.Map(argus[0])
        elif ops == 'join' and len(argus) == 3:
            return df.Join(table_data[argus[0]], argus[1], argus[2])
        elif ops == 'agg' and len(argus) == 2:
            return df.Agg(argus[0], argus[1])
        elif ops == 'groupby' and len(argus) == 3:
            return df.Groupby(argus[0], argus[1], argus[2])
        elif ops == 'column_calculate' and len(argus) == 3:
            return df.ColumnCalculate(argus[0], argus[1], argus[2])
        elif ops == 'value_calculate':
            if len(argus) == 2:
                return value_calculate.ValueCalculate(argus[0], argus[1])
            elif len(argus) == 3:
                return value_calculate.ValueCalculate(argus[0], argus[1], argus[2])
        elif ops == 'sem_map' and len(argus) == 1:
            processed_df = dedup(df, argus[0])
            mapped_df = processed_df.sem_map(argus[0])
            return postprocess_sem_map(df, mapped_df, argus[0])
        elif ops == 'sem_filter' and len(argus) == 1:
            processed_df = dedup(df, argus[0])
            filtered_df =  processed_df.sem_filter(argus[0])
            return postprocess_sem_filter(df, filtered_df)
        elif ops == 'sem_join' and len(argus) == 2:
            other_df = table_data[argus[0]]
            instruction = argus[1]
            processed_df, processed_other_df = dedup_join(df, other_df, instruction)
            if type(processed_df) == str:
                return "Invalid arguments: sem_join requires exactly two columns."
            flag = False
            if len(processed_df) > len(processed_other_df):
                processed_df, processed_other_df = processed_other_df, processed_df
                df, other_df = other_df, df
                instruction = instruction.replace(':left', ':left_temp').replace(':right', ':left').replace(':left_temp', ':right')
                flag = True
            result = processed_df.sem_join(processed_other_df, instruction)
            if flag:
                for column in result.columns:
                    if column.endswith(':left'):
                        result.rename(columns={column: column.replace(':left', ':right')}, inplace=True)
                    elif column.endswith(':right'):
                        result.rename(columns={column: column.replace(':right', ':left')}, inplace=True)
                df, other_df = other_df, df
                instruction = instruction.replace(':left', ':left_temp').replace(':right', ':left').replace(':left_temp', ':right')
            print(result)
            return postprocess_sem_join(result, df, other_df, instruction)
        elif ops == 'sem_agg' and len(argus) == 1:
            if '{' not in argus[0]:
                return df.sem_agg(argus[0], all_cols=True)
            return df.sem_agg(argus[0])
        elif ops == 'sem_topk' and len(argus) == 2:
            return df.sem_topk(argus[0], int(argus[1]))
        elif ops == 'sem_cluster_by' and len(argus) == 4:
            argus_index= argus[0] + "_index"
            clustered_df =  df.reset_index(drop=True).sem_index(argus[0],argus_index).sem_cluster_by(argus[0],int(argus[1]))
            aggregated_df = clustered_df.Groupby('cluster_id', argus[2], argus[3])
            # summarize argus[0] for each cluster_id
            for i in range(int(argus[1])):
                group = clustered_df[clustered_df['cluster_id'] == i]
                group_col = group[[argus[0]]]
                group_col = group_col.drop_duplicates()
                summary_df = group_col.sem_agg('Summarize {'+ argus[0] + '} in a short sentence.')
                summary = summary_df.iloc[0]['_output']
                if 'Answer: ' in summary:
                    summary = summary.split('Answer: ')[-1]
                # add summary to aggregated_df
                aggregated_df.loc[aggregated_df['cluster_id'] == i, 'summary'] = summary
            # delete cluster_id column
            aggregated_df = aggregated_df.drop(columns=['cluster_id'])
            aggregated_df = aggregated_df[['summary', f'{argus[2]}_{argus[3]}']]
            return aggregated_df
        else:
            return "Invalid arguments."
    except Exception as e:
        import traceback
        traceback.print_exc()
        return "Invalid arguments. Error message: " + str(e)

# Remove duplicates and NaN values from the columns involved in the semantic operation
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

# Remove duplicates and NaN values from the columns involved in the semantic join operation
def dedup_join(df, other_df, insturction):
    cols = lotus.nl_expression.parse_cols(insturction)
    if len(cols) != 2:
        return "Invalid arguments: sem_join requires exactly two columns."
    left_on = None
    right_on = None
    for col in cols:
        if ":left" in col:
            left_on = col.split(":left")[0]
        elif ":right" in col:
            right_on = col.split(":right")[0]
    if left_on is None or right_on is None:
        raise ValueError("Join columns not found in DataFrame")
    temp_df = df.copy()
    temp_other_df = other_df.copy()
    temp_df = temp_df[[left_on]]
    temp_other_df = temp_other_df[[right_on]]
    temp_df = temp_df.drop_duplicates()
    temp_other_df = temp_other_df.drop_duplicates()
    temp_df = temp_df.dropna()
    temp_other_df = temp_other_df.dropna()
    return temp_df, temp_other_df

# Post-process the result of semantic filter to ensure it aligns with the original dataframe
def postprocess_sem_filter(df, filtered_df):
    col_li = filtered_df.columns
    df = df.merge(filtered_df[col_li], on=list(col_li), how='inner')
    return df

# Post-process the result of semantic map to ensure it aligns with the original dataframe
def postprocess_sem_map(df, mapped_df, insturction):
    cols = lotus.nl_expression.parse_cols(insturction)
    df = df.merge(mapped_df, on=list(cols), how='inner')
    return df

# Post-process the result of semantic join to ensure it aligns with the original dataframes
def postprocess_sem_join(joined_df, df, other_df, insturction):
    cols = lotus.nl_expression.parse_cols(insturction)
    left_on = None
    right_on = None
    for col in cols:
        if ":left" in col:
            left_on = col.split(":left")[0]
        elif ":right" in col:
            right_on = col.split(":right")[0]
    df['key'] = 1
    other_df['key'] = 1
    total_df = pd.merge(df, other_df, on='key', suffixes=[":left", ":right"]).drop(columns='key')
    if left_on in other_df.columns:
        joined_df = joined_df.rename(columns={left_on: left_on + ":left"})
        left_on = left_on + ":left"
    if right_on in df.columns:
        joined_df = joined_df.rename(columns={right_on: right_on + ":right"})
        right_on = right_on + ":right"
    result = pd.merge(total_df, joined_df, on=[left_on, right_on], how='inner')
    return result
