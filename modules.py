import json
import base64
from datetime import datetime
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import os
from google.cloud import storage
import io

import sqlalchemy

def get_variables_dynamic(request):
    # Get the current date
    current_date = datetime.now()
    # Format the date as YY-MM-DD
    call_date = f'"{current_date.strftime("%Y-%m-%d")}"'
    formatted_date = current_date.strftime("%Y%m%d")

    request_json = request.get_json(silent=True)

    variables = {}

    if request_json and 'data' in request_json:
        eventdata = request_json['data']

        if 'bucket' in eventdata:
            variables['bucket'] = eventdata['bucket']
        else:
            variables['bucket'] = "meta_csv_test"

        if 'bucket_processed' in eventdata:
            variables['bucket_processed'] = eventdata['bucket_processed']
        else:
            variables['bucket_processed'] = "meta_processed"

        if 'name' in eventdata:
            variables['name'] = eventdata['name']
        else:
            variables['name'] = "metadata.csv"

        if 'target_table' in eventdata:
            variables['target_table'] = eventdata['target_table']
        else:
            variables['target_table'] = "metadata"

        if 'chunk_size' in eventdata:
            variables['chunk_size'] = eventdata['chunk_size']
        else:
            variables['chunk_size'] = 10000
    else:

        variables['bucket'] = "csv_test"
        variables['bucket_processed'] = "csv_processed"
        variables['name'] = "metadata.csv"
        variables['target_table'] = "metadata"
        variables['chunk_size'] = 10000

    return variables

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of Postgres.

    Uses the Cloud SQL Python Connector package.
    """

    instance_connection_name = os.environ[
        "INSTANCE_CONNECTION_NAME"
    ]
    db_user = os.environ["DB_USER"]  # e.g. 'my-db-user'
    db_pass = os.environ["DB_PASS"]  # e.g. 'my-db-password'
    db_name = os.environ["DB_NAME"]  # e.g. 'my-database'

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector(refresh_strategy="LAZY")

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )
        return conn

    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    return pool


def move_blob(source_bucket_name, destination_bucket_name, file_name):
    storage_client = storage.Client()

    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)

    destination_bucket = storage_client.bucket(destination_bucket_name)

    # Copy the blob
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, file_name
    )
    print(f"Blob {file_name} copied to {destination_bucket_name}.")

    # Delete the original blob
    source_blob.delete()
    print(f"Blob {file_name} deleted from {source_bucket_name}.")


def clean_header_line(text_stream):
    # Read the first line (header row), clean it, and rebuild the stream
    header_line = text_stream.readline()
    print(f"Header {header_line}.")
    clean_header = header_line.replace('\n', '').replace('\r', '')
    print(f"Header cleaned {clean_header}.")
    # Read the rest of the file
    remaining_lines = text_stream.read()

    # Combine cleaned header and original data back into a new stream
    cleaned_csv = io.StringIO(clean_header + '\n' + remaining_lines)
    print(cleaned_csv)
    return cleaned_csv

def datatypes():
    dtype_map = {
    'id': 'int32',
    'name': 'string',
    'cost': 'float32'
    }

    return dtype_map
