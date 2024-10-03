from elasticsearch import Elasticsearch
import pandas as pd

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Create an index (replace 'employees' with the desired index name)
index_name = 'employees'
es.indices.create(index=index_name, ignore=400)  # Ignore if index already exists

# Load employee data from a CSV file with specified encoding
df = pd.read_csv('/home/chinju/Downloads/Employee.csv', encoding='ISO-8859-1')

# Convert all fields to strings to prevent parsing issues
df = df.astype(str)

# Index the data into Elasticsearch
for i, row in df.iterrows():
    doc = row.to_dict()  # Convert each row to a dictionary
    es.index(index=index_name, body=doc)

print("Data indexed successfully!")

# Query the index to check if documents exist
res = es.search(index=index_name, body={"query": {"match_all": {}}})

# Print the number of documents and sample data
print(f"Total documents: {res['hits']['total']['value']}")
print("Sample data:", res['hits']['hits'][0]['_source'])













