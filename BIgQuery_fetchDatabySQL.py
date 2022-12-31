from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
from pandas.io import gbq

key_path = "digital-method-373108-d078eec51471.json"
project_id = "digital-method-373108"
dataset_id = "himanshu_demo_data"
table = "test"
table_id = "{}.{}.{}".format(project_id, dataset_id, table)
scopes=["https://www.googleapis.com/auth/cloud-platform"]

credentials = service_account.Credentials.from_service_account_file(key_path)

def GetCustomData(table_id):
    query = "SELECT * from "+table_id
    bigdata=gbq.read_gbq(query,project_id="shipmetrix",credentials=credentials)
    list_of_jsons = bigdata.to_json(orient='records', lines=True).splitlines()
    datas=[]
    for i in list_of_jsons:
        datas.append(json.loads(i))
    return datas

print(GetCustomData(table_id=table_id))

    
