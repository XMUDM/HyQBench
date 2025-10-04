import pandas as pd
import sys
sys.path.append("/data1/boli/tableRAG/code/lotus")
import val_ops.filter
import val_ops.orderby
import val_ops.head
import val_ops.map
import val_ops.join
import val_ops.agg
import val_ops.groupby
import val_ops.column_calculate
import val_ops.value_calculate
import sem_ops.sem_filter
sys.path.append("/data1/boli/tableRAG/code")
import lotus
from lotus.models import LM

data = {
    "Course Name": [
        "Probability and Random Processes",
        "Optimization Methods in Engineering",
        "Digital Design and Integrated Circuits",
        "Computer Security",
    ],
    "Course Number": [123, 456, 789, 101],
}

data2 = {
    "Student Name": [
        "Alice",
        "Bob",
        "Charlie"
    ],
    "Student ID": [123, 456, 789],
}

# df = pd.DataFrame(data)
# df2 = pd.DataFrame(data2)
# user_instruction = "{Course Name} requires a lot of math"
# df = df.Head(1)
# print(df)
df = pd.read_csv("/data1/boli/tableRAG/code/pandas_dfs/california_schools/satscores.csv")
# df = df.Filter("AvgScrMath", "gt", "560")
# print(df)
lm = LM(model="gpt-3.5-turbo")
lotus.settings.configure(lm=lm)
df = df.head(1000)
print(len(df['sname'].unique()))
df = df.sem_filter("{sname} is a school in the Bay Area")
# print(df)
# # 保存df
# df.to_csv("/Users/lee/Graduate/Lab/tableRAG/code/satscores.csv")
# df = df.Orderby(["Course Name", "Course Number"], ascending=False)
# print(df)
# # df = df.Map(["Course Name"])
# # print(df)
# df = df.Join(df2, "Course Number", "Student ID")
# print(df)
# # df = df.Agg("Course Number", "max")
# # print(df)
# # df = df.Groupby(["Course Name"])
# # print(df['Course Number'].sum())

# df = df.Calculate("Course Number", "Student ID", "add")
# print(df)
try:
    df1 = pd.read_csv("/Users/lee/Graduate/Lab/tableRAG/code/pandas_dfs/california_schools/satscores.csv")
    # df2 = pd.read_csv("/Users/lee/Graduate/Lab/tableRAG/code/pandas_dfs/california_schools/schools.csv")
    # df1 = df1.Map("[sname, dname, AvgScrRead]")
    # print(df1)
    # df = df1.Groupby("[AdmFName1]", "AdmFName1", "count")
    # print(df)
    # print(isinstance(df, pd.DataFrame))
    # print(type(df))
    # df = df1.Filter("AdmFName1", "like", "A%")
    df = df1.Filter("AvgScrRead", [("gt", 560), ("lt", 570)])
    print(df)
    # df = df1.Calculate("Consumption", "Consumption", "sub")
    # df = df1['Consumption'] - (df1['Consumption'])
    # df = df.Agg("AvgScrRead", "count")
# print(df)
except Exception as e:
    print(e)
# str1 = "[CDSCode, School, AvgScrRead, Phone]"
# print([i.strip() for i in str1[1:-1].split(',')])
