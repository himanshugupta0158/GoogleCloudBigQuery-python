import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'digital-method-373108-d078eec51471.json'
client = bigquery.Client('digital-method-373108')

# Add Labels
dataset_ref = bigquery.DatasetReference(client.project, "himanshu_demo_data")
table_ref = bigquery.TableReference(dataset_ref, "test")

data = {
    "std_name" : "James",
    "std_class" : "cloud_computing",
    "college" : "ABC",
    "location" : "vns"
}

# a> create labels
labels = {
    'type' : 'social_media',
    'category' : 'cloud_computing'
}


test_Table = client.get_table(table_ref)
test_Table.labels = labels

client.update_table(test_Table, ['labels'])  # its updates the table with labels.

# b> add Labels to the table reference

new_labels = {
    'type' : 'students details',
    'year' : '2022'
}
table = client.get_table(table_ref)
table.labels = new_labels
client.update_table(table, ['labels'])