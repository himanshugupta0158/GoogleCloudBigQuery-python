import datetime
from google.cloud import bigquery
import pandas as pd
import pytz
from google.oauth2 import service_account

key_path = "digital-method-373108-d078eec51471.json"
project_id = "digital-method-373108"
dataset_id = "himanshu_demo_data"
table = "test"
table_id = "{}.{}.{}".format(project_id, dataset_id, table)

print("************* Name Of The Table Is : ",table_id+"****************")

# To load/save new data into GoogleCLoud BigQuery Database table named 'test'
def load_table_dataframe_config(key_path, project_id, table_id, data):

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Construct a BigQuery Client Objects.
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
    )

    dataframe = pd.DataFrame(
        data,
        # IN the loaded table, the column order reflects the order of the 
        # columns in the Dataframe.
        columns=[
            "std_name",
            "std_class",
            "college",
            "location",
        ],
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        dataframe,
        table_id,
        job_config
    )

    print(job.result())

    data = client.get_table(table_id)

    return data



data = [
        {
            "std_name" : "Jai",
            "std_class" : "cloud_computing",
            "college" : "ABC",
            "location" : "vns"
        },
        {
            "std_name" : "Shyam",
            "std_class" : "BigData",
            "college" : "ABC",
            "location" : "delhi"
        },
        {
            "std_name" : "Shivam",
            "std_class" : "Python programming Language",
            "college" : "ABC",
            "location" : "vns"
        },
        {
            "std_name" : "Harsh",
            "std_class" : "Software Testing",
            "college" : "ABC",
            "location" : "Mumbai"
        },
    ]
try:
    print(load_table_dataframe_config(key_path, project_id, table_id, data))
except Exception as e:
    print(e)



