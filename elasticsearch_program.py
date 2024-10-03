import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

# Initialize Elasticsearch
es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])  # Adjust scheme, host, and port as needed

# Function to create a collection (index)
def create_collection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

# Function to index data
def index_data(p_collection_name, p_exclude_column):
    # Load the CSV file
    df = pd.read_csv('/home/chinju/Downloads/Employee.csv', encoding='ISO-8859-1')
    
    # Drop the excluded column if it exists
    if p_exclude_column in df.columns:
        df = df.drop(columns=[p_exclude_column])
    
    # Prepare actions for bulk indexing
    actions = [
        {
            "_index": p_collection_name,
            "_id": row['Employee ID'],
            "_source": row.to_dict()
        }
        for _, row in df.iterrows()
    ]

    try:
        # Bulk index the data
        helpers.bulk(es, actions)
        print(f"Indexed {len(actions)} documents into '{p_collection_name}'.")
    except BulkIndexError as e:
        print(f"Failed to index documents: {e.errors}")

# Function to count employees in the collection
def get_emp_count(p_collection_name):
    count = es.count(index=p_collection_name)['count']
    print(f"Employee count in '{p_collection_name}': {count}")

# Function to delete an employee by ID
def del_emp_by_id(p_collection_name, p_employee_id):
    try:
        res = es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Deleted employee with ID '{p_employee_id}' from '{p_collection_name}'. Result: {res['result']}")
    except Exception as e:
        print(f"Error deleting employee with ID '{p_employee_id}': {str(e)}")

# Function to search by column
def search_by_column(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {p_column_name: p_column_value}
        }
    }
    results = es.search(index=p_collection_name, body=query)
    print(f"Search results for '{p_column_name}' = '{p_column_value}': {results['hits']['hits']}")

# Function to get department facet
def get_dep_facet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword",
                    "size": 10  # Adjust size based on expected unique departments
                }
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    print("Department facet results:", response['aggregations']['departments']['buckets'])

# Function to get all employees
def get_all_employees(p_collection_name, size=10):
    query = {
        "query": {
            "match_all": {}
        }
    }
    results = es.search(index=p_collection_name, body=query, size=size)
    print(f"Top {size} employees in '{p_collection_name}':")
    for hit in results['hits']['hits']:
        print(f"ID: {hit['_id']}, Data: {hit['_source']}")
def get_high_salary_employees(p_collection_name, min_salary):
    query = {
        "query": {
            "range": {
                "Annual Salary": {
                    "gte": min_salary
                }
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    print(f"Employees with annual salary >= ${min_salary}:")
    for hit in results['hits']['hits']:
        print(f"ID: {hit['_id']}, Data: {hit['_source']}")

# Function Executions
if __name__ == "__main__":
    # Define collection names
    v_name_collection = 'hash_chinju'  # Replace 'chinju' with your name
    v_phone_collection = 'hash_1234'    # Replace '1234' with your phone last four digits

    create_collection(v_name_collection)
    create_collection(v_phone_collection)
    
    get_emp_count(v_name_collection)
    
    index_data(v_name_collection, 'Department')
    index_data(v_phone_collection, 'Gender')
    
    get_all_employees(v_name_collection)  # Check the records before deletion
    
    del_emp_by_id(v_name_collection, 'E02003')
    
    get_emp_count(v_name_collection)
    
    search_by_column(v_name_collection, 'Department', 'IT')
    search_by_column(v_name_collection, 'Gender', 'Male')
    search_by_column(v_phone_collection, 'Department', 'IT')
    get_high_salary_employees(v_name_collection, 80000)
    
    get_dep_facet(v_name_collection)
    get_dep_facet(v_phone_collection)

