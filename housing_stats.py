# pip install google-cloud-storage

import pandas as pd
import requests
import zipfile
import os
import sqlite3
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

credentials_path = os.getenv("credentials_path")
project_id = os.getenv("project_id")
bucket_name = os.getenv("bucket_name")
base_path = os.getcwd()

# Explicitly provide service account credentials to the client library
credentials = service_account.Credentials.from_service_account_file(credentials_path)

SOURCE_FILE_URL = "https://public.madd.bfs.admin.ch/ch.zip"
ZIP_FILE_NAME = "ch_data.zip"
SQLITE_DB_FILE_NAME = 'data.sqlite'


response = requests.get(SOURCE_FILE_URL)
if response.status_code == 200:
    
    # Save the downloaded ZIP file
    with open(ZIP_FILE_NAME, "wb") as file:
        file.write(response.content)
    print("File downloaded successfully!")

    # Extract the 'data.sqlite' file from the ZIP
    with zipfile.ZipFile(ZIP_FILE_NAME, "r") as zip_ref:
        zip_ref.extract(SQLITE_DB_FILE_NAME, path=base_path)

    # Remove the ZIP file
    os.remove(ZIP_FILE_NAME)
    print("ZIP file deleted.")
else:
    print("Failed to download the file.")


def generate_df(sqlite_table):
    # Read sqlite query results into a pandas DataFrame
    try:
        con = sqlite3.connect(SQLITE_DB_FILE_NAME)

        if con is not None:
            print("Connection to SQLite database successful.")
            df = pd.read_sql_query(f"SELECT * FROM {sqlite_table}", con)
            con.close()
        else:
            print("Failed to connect to SQLite database.")
            return None
        
        print(f"{sqlite_table} Dataframe generated successfully!")        
        return df
    
    except Exception as e:
        print(e)


sqlite_tables = ["codes", "building", "dwelling", "entrance"]


codes_df, building_df, dwelling_df, entrance_df = [generate_df(i) for i in sqlite_tables]

def store_parquet_on_gcp(df, bucket_name, parquet_df_name):
    print(f'Creating {parquet_df_name}')
    df.to_parquet(parquet_df_name)
    blob_name = f'api_housing_stats/{parquet_df_name}'
    
    try:
        print(f"Uploading {parquet_df_name} to GCP")
        # Initialize a client

        client = storage.Client(credentials=credentials)

        # Get the bucket
        bucket = client.bucket(bucket_name)

        # Create a new blob and upload the file
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(parquet_df_name, timeout=300)

        print(f"File {blob_name} uploaded to {bucket_name}.")
    
    except Exception as e:
        
        print(f"Failed to upload. Error: {e}")

    print(f"Removing {parquet_df_name} locally")
    os.remove(parquet_df_name)

try:
    store_parquet_on_gcp(codes_df, bucket_name, 'housing_stats_codes.parquet')
    store_parquet_on_gcp(building_df, bucket_name, 'housing_stats_buildings.parquet')
    store_parquet_on_gcp(dwelling_df, bucket_name, 'housing_stats_dwelling.parquet')
    store_parquet_on_gcp(entrance_df, bucket_name, 'housing_stats_entrance.parquet')
except Exception as e:
    print(f"An error has appeard: {e}")
finally:
    if os.path.exists(SQLITE_DB_FILE_NAME):
        os.remove(SQLITE_DB_FILE_NAME)

