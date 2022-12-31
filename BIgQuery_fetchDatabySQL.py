import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'digital-method-373108-d078eec51471.json'
client = bigquery.Client()


sql = """
SELECT * FROM digital-method-373108.himanshu_demo_data.test2
LIMIT 5;
"""


query = client.query(
    sql,
    location='US',
    job_config=bigquery.QueryJobConfig(maximum_bytes_billed=50_000_000),
    job_id_prefix="job__high_quality_process"
)

# print(query)
# method 1 : to retrive records
for row in query.result():
    print("{\nName : ",row.name)
    print("Gender : ",row.gender)
    print("Count : ",row.count,"\n}")


# method 2 : to retrive records (to dataframe)

df = query.to_dataframe()
print(df)