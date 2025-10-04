import pandas as pd
import sys
sys.path.append("/Users/lee/Graduate/Lab/tableRAG/code/lotus")
import sem_ops.sem_cluster_by
sys.path.append("/Users/lee/Graduate/Lab/tableRAG/code")
import lotus
from lotus.models import LM, SentenceTransformersRM

lm = LM(model="gpt-3.5-turbo-0125")
rm = SentenceTransformersRM(model="/Users/lee/Graduate/Lab/tableRAG/code/solution/model/bert-base-nli-mean-tokens")

lotus.settings.configure(lm=lm, rm=rm)
data = {
    "Course Name": [
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
    ]
}
df = pd.DataFrame(data)
df = df.sem_index("Course Name", "course_name_index").sem_cluster_by("Course Name", 1)
print(df)