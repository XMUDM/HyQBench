import json
import sys
sys.path.append('..')
from dataGeneration.scripts.tool_pool import tools_simplified

class preprocessor:
    def __init__(self, model):
        self.model = model

    def preprocess_table(self, tables, query):
        prompt = f"""
You are a database table preprocessor, which needs to determine which columns of the table are useful based on the query. If you think all columns are useful, you can give '*' as the result of 'columns'.

The operators that may be used in a query are as follows
{json.dumps(tools_simplified, indent=4)}

Some information cannot be directly obtained from the table and may require inference by combining columns in the table and external knowledge, and these columns are also useful.

### Example 1:
Query:
What are the titles of the posts owned by user csgillespie that are about databases?

Table data:
{{
    "users": {{
        "*": "all columns",
        "Id": "description: column_description=the user id; value_description=NULL dtype=int64 e.g. 401; 3438; 40067; 21691; 34658; 2933",
        "Reputation": "description: column_description=the user's reputation; value_description=commonsense evidence: The user with higher reputation has more influence.  dtype=int64 e.g. 4128; 63; 145; 6906; 96; 932",
        "CreationDate": "description: column_description=the creation date of the user account; value_description=NULL dtype=object e.g. 2012-08-19 21:12:41; 2012-08-27 06:05:20; 2012-04-21 10:26:53; 2014-08-19 11:02:52; 2013-11-11 18:36:47; 2012-06-05 21:15:49",
        "DisplayName": "description: column_description=the user's display name; value_description=NULL dtype=object e.g. Ari B. Friedman; Newb; Ivan; DS1; omatai; user3638780",
        "LastAccessDate": "description: column_description=the last access date of the user account; value_description=NULL dtype=object e.g. 2014-04-14 17:18:41; 2012-11-19 06:57:35; 2014-09-12 10:24:02; 2014-03-27 16:32:27; 2014-09-13 20:24:26; 2014-09-12 05:22:41",
        "WebsiteUrl": "description: column_description=the website url of the user account; value_description=NULL dtype=object e.g. http://massd.github.io; http://tgoossens.wordpress.com; http://www.depthfirstsearch.net; http://www.dagstuhl.de; http://www.labsens.com/; http://statisticaconr.blogspot.com",
        "Location": "description: column_description=user's location; value_description=NULL dtype=object e.g. Atlanta, GA; Paris, France; Bonn, Germany; San Francisco, CA; Delaware; The Earth",
        "AboutMe": "description: column_description=the self introduction of the user; value_description=NULL dtype=object e.g. <p>Professor of Statistics, Monash University, Australia. Have used R and LaTeX for more than 20 years.</p>\n",
        "Views": "description: column_description=the number of views ; value_description=NULL dtype=int64 e.g. 2; 4; 164; 344; 1244; 127",
        "UpVotes": "description: column_description=the number of upvotes; value_description=NULL dtype=int64 e.g. 138; 7; 77; 17; 288; 80",
        "DownVotes": "description: column_description=the number of downvotes; value_description=NULL dtype=int64 e.g. 4; 6; 2; 1; 14; 13",
        "AccountId": "description: column_description=the unique id of the account; value_description=NULL dtype=int64 e.g. 1609799; 1751939; 48509; 206086; 2784324; 139823",
        "Age": "description: column_description=user's age; value_description= teenager: 13-18  adult: 19-65  elder: > 65 dtype=float64 e.g. 31.0; 32.0; 22.0; 23.0; 37.0; 47.0",
        "ProfileImageUrl": "description: column_description=the profile image url; value_description=NULL dtype=object e.g. http://i.stack.imgur.com/Gakek.jpg?s=128&g=1; https://www.gravatar.com/avatar/35e8672292bbff480a3f0426ffa1510c?s=128&d=identicon&r=PG; https://www.gravatar.com/avatar/f1f024ede163fdaebe61807ad5c88f1e?s=128&d=identicon&r=PG&f=1; https://www.gravatar.com/avatar/c45b21d940a6c5b78da7205268b60604?s=128&d=identicon&r=PG; https://www.gravatar.com/avatar/0d5ec28577725dc5689571d249a76105?s=128&d=identicon&r=PG; http://i.stack.imgur.com/KQQh9.jpg"
    }},
    "posts": {{
        "*": "all columns",
        "Id": "description: column_description=the post id; value_description=NULL dtype=int64 e.g. 13644; 32839; 33249; 8899; 60055; 96095",
        "PostTypeId": "description: column_description=the id of the post type; value_description=NULL dtype=int64 e.g. 2; 1",
        "AcceptedAnswerId": "description: column_description=the accepted answer id of the post ; value_description=NULL dtype=float64 e.g. 28802.0; 28503.0; 70822.0; 80006.0; 80292.0; nan",
        "CreaionDate": "description: column_description=the creation date of the post; value_description=NULL dtype=object e.g. 2012-02-27 16:12:09; 2014-04-07 01:40:12; 2011-07-31 07:52:20; 2012-07-05 20:54:18; 2014-04-13 08:17:21; 2011-07-14 10:33:40",
        "Score": "description: column_description=the score of the post; value_description=NULL dtype=int64 e.g. 6; 49; 20; 4; 123; 18",
        "ViewCount": "description: column_description=the view count of the post; value_description=commonsense evidence: Higher view count means the post has higher popularity dtype=float64 e.g. 257.0; 248.0; 547.0; 112.0; 2641.0; 53.0",
        "Body": "description: column_description=the body of the post; value_description=NULL dtype=object e.g. <p>Try:</p>\n\n<pre><code>library(ez)\nezANOVA(data=subset(p12bl, exps==1),\n  within=.(sucrose, citral),\n  wid=.(subject),\n  dv=.(resp)\n  )\n</code></pre>\n\n<p>and the equivalent aov command, minus sphericity etc, is:</p>\n\n<pre><code>aov(resp ~ sucrose*citral + Error(subject/(sucrose*citral)), \n    data= subset(p12bl, exps==1))\n</code></pre>\n\n<p>Here's the equivalent using Anova from car directly:</p>\n\n<pre><code>library(car)\ndf1&lt;-read.table(\"clipboard\", header=T) #From copying data in the question above\nsucrose&lt;-factor(rep(c(1:4), each=4))\ncitral&lt;-factor(rep(c(1:4), 4))\nidata&lt;-data.frame(sucrose,citral)\n\nmod&lt;-lm(cbind(S1C1, S1C2, S1C3, S1C4, S2C1, S2C2, S2C3, S2C4, \n        S3C1, S3C2, S3C3, S3C4, S4C1, S4C2, S4C3, S4C4)~1, data=df1)\nav.mod&lt;-Anova(mod, idata=idata, idesign=~sucrose*citral)\nsummary(av.mod)\n</code></pre>\n",
        "OwnerUserId": "description: column_description=the id of the owner user; value_description=NULL dtype=float64 e.g. 14443.0; 11947.0; 2040.0; 919.0; 1670.0; 9326.0",
        "LasActivityDate": "description: column_description=the last activity date; value_description=NULL dtype=object e.g. 2011-07-30 13:35:21; 2013-01-28 22:20:47; 2013-04-29 13:07:06; 2014-03-11 17:43:20; 2012-01-18 08:20:53; 2013-12-24 08:31:55",
        "Title": "description: column_description=the title of the post; value_description=NULL dtype=object e.g. Using Conditional Optimizations in Training Algorithm of Linear Support Vector Classifier; Best test to assess statistical difference between two groups on a set of 5-point questionnaire items; What method is used in Google's correlate?; What machine learning techniques are especially susceptible to \"over-tuning\" of their hyperparameters?; Spherical platykurtic random cloud; Incorrect reasoning in obtaining average family income",
        "Tags": "description: column_description=the tag of the post; value_description=NULL dtype=object e.g. <time-series><correlation>; <probability><self-study>; <time-series><books>; <multivariate-analysis><univariate>; <bayesian><self-study><normal-distribution><conditional-probability>; <regression><data-transformation>",
        "AnswerCount": "description: column_description=the total number of answers of the post; value_description=NULL dtype=float64 e.g. 0.0; 3.0; 6.0; 4.0; nan; 5.0",
        "CommentCount": "description: column_description=the total number of comments of the post; value_description=NULL dtype=int64 e.g. 11; 12; 15; 1; 6; 13",
        "FavoriteCount": "description: column_description=the total number of favorites of the post; value_description=commonsense evidence: more favorite count refers to more valuable posts.  dtype=float64 e.g. 3.0; 10.0; 15.0; 0.0; 4.0; 6.0",
        "LastEditorUserId": "description: column_description=the id of the last editor; value_description=NULL dtype=float64 e.g. 1540.0; 8076.0; 44009.0; 5739.0; 5494.0; 686.0",
        "LastEditDate": "description: column_description=the last edit date; value_description=NULL dtype=object e.g. 2013-09-22 09:00:47; 2012-09-15 19:11:10; 2013-12-18 12:04:38; 2014-03-13 05:56:26; 2014-04-18 01:24:47; 2013-09-21 22:28:33",
        "CommunityOwnedDate": "description: column_description=the community owned date; value_description=NULL dtype=object e.g. nan; 2012-10-02 17:46:38",
        "ParentId": "description: column_description=the id of the parent post; value_description=commonsense evidence: If the parent id is null, the post is the root post. Otherwise, the post is the child post of other post.  dtype=float64 e.g. 58817.0; 80834.0; 27446.0; 21655.0; 18590.0; 96091.0",
        "ClosedDate": "description: column_description=the closed date of the post; value_description=commonsense evidence: if ClosedDate is null or empty, it means this post is not well-finished if CloseDate is not null or empty, it means this post has well-finished. dtype=object e.g. nan; 2012-05-10 07:49:25; 2013-06-25 19:35:44; 2013-07-03 19:08:23; 2014-04-18 00:42:27",
        "OwnerDisplayName": "description: column_description=the display name of the post owner; value_description=NULL dtype=object e.g. nan",
        "LastEditorDisplayName": "description: column_description=the display name of the last editor; value_description=NULL dtype=object e.g. nan; user28"
    }}
}}

Result:
[
    {{
        "table_name": "posts",
        "thought": "The query requires fetching the titles of posts related to databases for the user 'csgillespie'. We need to identify the user's posts, so we need to retain some columns to connect with the users table. Since there is no direct information in the table indicating whether the posts are related to databases, we need to extract the 'title' column and filter it semantically."
        "columns": "[Id, Title, OwnerUserId]"
    }},
    {{
        "table_name": "users",
        "thought": "To connect the posts to the correct user, we need to retrieve the user's 'Id' and match it with the 'OwnerUserId' in the posts table.",
        "columns": "[Id, DisplayName]"
    }}
]

### Example 2:
Query:
In the SAT scores, Which ones have a AvgScrMath greater than 500?

Table data:
{{
    "satscores": {{
        "*": "all columns",
        "cds": "description: column_description=California Department Schools; value_description=NULL dtype=int64 e.g. 33672490000000; 10751270000000; 34673303434792; 30736503030285; 30666703035821; 33752423330552",
        "rtype": "description: column_description=rtype; value_description=unuseful dtype=object e.g. S; D",
        "sname": "description: column_description=school name; value_description=NULL dtype=object e.g. La Serna High; Mammoth High; Mountain Oaks; Upland High; Provisional Accelerated Learning Academy; Herbert Hoover High",
        "dname": "description: column_description=district segment; value_description=NULL dtype=object e.g. Westside Elementary; Anaheim Union High; La Honda-Pescadero Unified; Kern High; Santa Clara County Office of Education; Santa Maria Joint Union High",
        "cname": "description: column_description=county name; value_description=NULL dtype=object e.g. Alameda; Kern; San Diego; San Mateo; Orange; Contra Costa",
        "enroll12": "description: column_description=enrollment (1st-12nd grade); value_description=NULL dtype=int64 e.g. 1684; 209; 142; 224; 62; 136",
        "NumTstTakr": "description: column_description=Number of Test Takers in this school; value_description=number of test takers in each school dtype=int64 e.g. 139; 226; 147; 224; 394; 144",
        "AvgScrRead": "description: column_description=average scores in Reading; value_description=average scores in Reading dtype=float64 e.g. 450.0; 476.0; 423.0; 407.0; 460.0; 591.0",
        "AvgScrMath": "description: column_description=average scores in Math; value_description=average scores in Math dtype=float64 e.g. 451.0; 437.0; 600.0; 488.0; 510.0; 512.0",
        "AvgScrWrite": "description: column_description=average scores in writing; value_description=average scores in writing dtype=float64 e.g. 555.0; 498.0; 484.0; 433.0; 532.0; 529.0",
        "NumGE1500": "description: column_description=Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500; value_description=Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500  commonsense evidence:  Excellence Rate = NumGE1500 / NumTstTakr dtype=float64 e.g. 76.0; 158.0; 807.0; 762.0; 187.0; 630.0"
    }}
}}

Result:
[
    {{
        "table_name": "satscores",
        "thought": "All the information in the table will be shown in the result, so we can keep all columns.",
        "columns": "*"
    }}
]

### Your Task
Query:
{query}

Table data:
{json.dumps(tables, indent=4)}

Your output will be strictly matched by the code, so do not output any other content. The format of the output should be as follows:
[
    {{
        "table_name": "table_name",
        "thought": "thought on the query and the columns to be retained",
        "columns": "[column1, column2, ...] or *"
    }},
    {{
        "table_name": "table_name",
        "thought": "thought on the query and the columns to be retained",
        "columns": "[column1, column2, ...] or *"
    }},
    ...
]
        """

        response = self.model(prompt)
        if response.startswith('```json'):
            response = response.split('```json')[1].split('```')[0].strip()
        response = eval(response)
        llm_selected = {}
        for item in response:
            llm_selected[item['table_name']] = item['columns']
    
        name = list(tables[0].keys())[0]
        if name != 'arxiv' and name != 'amazon_review' and name != 'news_category' and name != 'microsoft_news':
            with open('../dataGeneration/data/BIRD/dev_tables.json', 'r', encoding='utf-8') as f:
                dbs = json.load(f)
                for db in dbs:
                    if name in db['table_names_original']:
                        target_db = db
                        break
                keys = {table_name: [] for table_name in db['table_names_original']}
                for i in range(len(target_db['primary_keys'])):
                    if type(target_db['primary_keys'][i]) == list:
                        for col in target_db['primary_keys'][i]:
                            keys[db['table_names_original'][target_db['column_names_original'][col][0]]].append(target_db['column_names_original'][col][1])
                    else:
                        keys[db['table_names_original'][target_db['column_names_original'][target_db['primary_keys'][i]][0]]].append(target_db['column_names_original'][target_db['primary_keys'][i]][1])

                for i in range(len(target_db['foreign_keys'])):
                    for col in target_db['foreign_keys'][i]:
                        column = target_db['column_names_original'][col]
                        table_name = target_db['table_names_original'][column[0]]
                        column_name = column[1]
                        if column_name not in keys[table_name]:
                            keys[table_name].append(column_name)
        else:
            keys = {name: []}

        result = []

        for item in tables:
            name = list(item.keys())[0]
            columns = list(item[name].keys())
            selected_columns = []
            print(llm_selected[name])
            if '*' in llm_selected[name]:
                selected_columns = [col for col in columns if col != '*']
            else:
                for key in keys[name]:
                    selected_columns.append(key)
                for column in columns:
                    if column in llm_selected[name] and column not in selected_columns:
                        selected_columns.append(column)
            result.append({
                "table_name": name,
                "columns": selected_columns
            })

        return result
