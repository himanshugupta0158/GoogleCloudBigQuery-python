from google.cloud import bigquery
import pandas as pd
import names
from random import randint

SERVICE_ACCOUNT_JSON = 'digital-method-373108-d078eec51471.json'

def data_generator(num):
    data = []
    gender = ["male","male", "female","female","male", "female","male", "female"]
    for i in range(num):
        # print(names.get_full_name())
        g = gender[randint(0,7)]
        data.append({
            "name" : names.get_full_name(gender=g),
            "gender" : g,
            "count" : int(i)+1
        })
    dataframe = pd.DataFrame(
        data,
        columns=[
            "name",
            "gender",
            "count"
        ]
    )
    return dataframe


def create_table(table_id):
    # Construct a BigQuery Client Object.
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("count", "INTEGER")
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)

    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def load_table_to_bq(table_id, data):
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

    job_config = bigquery.LoadJobConfig(

    )

    
    # print(source_file)

    # data = data_generator(10)
    # print(data)

    job = client.load_table_from_dataframe(data, table_id, job_config)

    job.result()

    table = client.get_table(table_id)

    print("Loaded {} rows and table is {}".format(table.num_rows, table_id))



def get_table(table_id):
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

    table = client.get_table(table_id)

    # View table properties
    print("Got Table '{}.{}.{}'".format(table.project, table.dataset_id, table.table_id))
    print("Table Schema : {}".format(table.schema))
    print("Table Description : {}".format(table.description))
    print("Table has {} rows".format(table.num_rows))

    

def list_tables(dataset_id):
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

    tables = client.list_tables(dataset_id)
    print("Tables contained in '{}'".format(dataset_id))

    for table in tables:
        print("Got Table '{}.{}.{}'".format(table.project, table.dataset_id, table.table_id))


def delete_table(table_id):
    # Construct a BigQuery CLient object.
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

    client.delete_table(table_id, not_found_ok=True) #Make an API request
    print("Deleted Table '{}'".format(table_id))


if __name__ == "__main__":
    table_id = "digital-method-373108.himanshu_demo_data.test2"
    dataset_id = "digital-method-373108.himanshu_demo_data"
    # create_table(table_id)
    # data = data_generator(30)
    # print(data)
    # load_table_to_bq(table_id, data)
    # get_table(table_id)
    # list_tables(dataset_id)
    # delete_table(table_id)

