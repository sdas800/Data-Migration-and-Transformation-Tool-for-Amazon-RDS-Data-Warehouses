import requests
import zipfile
import os
import json
import boto3
import pandas as pd
from sqlalchemy import create_engine


# Function to download zip file from URL
def download_zip_file(url, destination):
    response = requests.get(url)
    with open(destination, 'wb') as f:
        f.write(response.content)


# Function to extract JSON files from the zip file
def extract_json_files(zip_file_path, extraction_path):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extraction_path)


# Function to upload JSON files to Amazon S3
def upload_to_s3(json_files_directory, bucket_name):
    s3 = boto3.client('s3')
    for file_name in os.listdir(json_files_directory):
        file_path = os.path.join(json_files_directory, file_name)
        s3.upload_file(file_path, bucket_name, file_name)


# Function to load data from Amazon S3 into Amazon RDS using pandas and sqlalchemy
def load_into_rds(bucket_name, aws_access_key_id, aws_secret_access_key, rds_database_url, table_name):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    object_list = s3.list_objects(Bucket=bucket_name)['Contents']
    for obj in object_list:
        if obj['Key'].endswith('.json'):
            file_name = obj['Key']
            response = s3.get_object(Bucket=bucket_name, Key=file_name)
            data = json.loads(response['Body'].read().decode('utf-8'))
            df = pd.DataFrame(data)
            engine = create_engine(rds_database_url)
            df.to_sql(table_name, engine, if_exists='append', index=False)


# Example usage
if __name__ == "__main__":
    zip_file_url = "URL_OF_YOUR_ZIP_FILE"
    zip_file_destination = "local_zip_file.zip"
    extraction_path = "extracted_json_files"
    bucket_name = "your-s3-bucket-name"
    aws_access_key_id = "YOUR_AWS_ACCESS_KEY_ID"
    aws_secret_access_key = "YOUR_AWS_SECRET_ACCESS_KEY"
    rds_database_url = "RDS_DATABASE_URL"
    table_name = "your_table_name"

    download_zip_file(zip_file_url, zip_file_destination)
    extract_json_files(zip_file_destination, extraction_path)
    upload_to_s3(extraction_path, bucket_name)
    load_into_rds(bucket_name, aws_access_key_id, aws_secret_access_key, rds_database_url, table_name)
