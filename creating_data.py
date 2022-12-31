from random import randint
import names
import json
import csv

data = {"data" : []}
gender = ["male", "female"]
for i in range(20):
    # print(names.get_full_name())
    data["data"].append({
        "name" : names.get_full_name(),
        "gender" : gender[randint(0,1)],
        "count" : int(i)+1
    })

jsondata = json.dumps(data["data"])
jsonFile = open("sample_data.json", "w")
jsonFile.write(jsondata) # json file created
jsonFile.close()

csv_file = open('sample_data.csv', 'w')

csv_writer = csv.writer(csv_file)

# print(data["data"])

count = 0

for d in data["data"]:
    if count == 0:
        header = d.keys()
        csv_writer.writerow(header)
        count += 1

    csv_writer.writerow(d.values())

csv_file.close()