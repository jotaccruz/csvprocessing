import functions_framework
from modules import *
import csv
from google.cloud import storage
import io
import pandas as pd
import time

@functions_framework.http
def csvprocessing_https(request):

    variables = get_variables_dynamic(request)

    message = process_csv(variables)

    return message

def process_csv(variables):
    bucket_name = variables['bucket']
    file_name = variables['name']

    if not file_name.endswith('.csv'):
        print(f"Skipping non-CSV file: {file_name}")
        return

    print(f"Streaming file: {file_name} from bucket: {bucket_name}")

    # Start timer
    start_time = time.time()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    engine = connect_with_connector()

    print(f"Streaming file: {file_name} from bucket: {bucket_name} in {variables['chunk_size']} chunk size")

    with blob.open("rb") as binary_stream:
        buffered_stream = io.BufferedReader(binary_stream)
        text_stream = io.TextIOWrapper(buffered_stream, encoding='utf-8', newline='')
        df = pd.read_csv(text_stream, chunksize = int(variables['chunk_size']))

        # Load data to PostgreSQL here...

        for i, chunk in enumerate(df):
            if i == 0:
                print(f"Inserting chunk 1")
                chunk.to_sql(
                    name=variables['target_table'],
                    con=engine,
                    if_exists="replace",
                    index=False,
                    method="multi",
                    chunksize = 1000
                )
            else:
                print(f"Inserting chunk {i+1}")
                chunk.to_sql(
                    name=variables['target_table'],
                    con=engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize = 1000
                )

    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Streaming file: {file_name} from bucket: {bucket_name} took {elapsed_time} secs")

    # Then move the processed file

    move_blob(
        source_bucket_name=variables['bucket'],
        destination_bucket_name=variables['bucket_processed'],
        file_name=variables['name']
    )

    return (f"Streaming file: {file_name} from bucket: {bucket_name} took {elapsed_time} secs '\n'")
