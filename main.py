import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np
from google.cloud import storage

bucket_name = "prediswiss-parquet-data"
bucket_row = "prediswiss-raw-data"

def main():
    storage_client = storage.Client(project="prediswiss")

    try:
        bucketParquet = storage_client.get_bucket(bucket_name)
    except:
        bucketParquet = create_bucket(bucket_name, storage_client)

    try:
        bucketRaw = storage_client.get_bucket(bucket_row)
    except:
        raise RawDataException
    


    #{heure : {CH:0542.05 : [120,0,23], CH:0026.02 : [23, 1, 123]}},{heure : {CH:0232.05 : [10,10,53], CH:0223.01 : [65, 12, 73]}}
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'example.parquet')

if __name__ == "__main__":
    main()

def create_bucket(name, client: storage.Client):    
    bucket = client.create_bucket(name, location="us-east1")
    print(f"Bucket {name} created")
    return bucket

def create_blob(root_bucket: storage.Bucket, destination_name, data_type, data):
    blob = root_bucket.blob(destination_name)
    generation_match_precondition = 0
    blob.upload_from_string(data, data_type, if_generation_match=generation_match_precondition)
    print("file created")

class RawDataException(Exception):
    "Raised when ubucket of raw data doesn't exist"
    pass