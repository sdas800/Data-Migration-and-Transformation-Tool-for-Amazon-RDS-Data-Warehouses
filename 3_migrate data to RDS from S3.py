import boto3
import json
import pandas as pd
from sqlalchemy import create_engine
import os

# Define function to flatten JSON data
def flatten_json(data, file_name):
    """Flatten the JSON data."""
    flattened_data = {
        'id': os.path.splitext(os.path.basename(file_name))[0],  # Using file's base name as ID
        'cik': data['cik'],
        'entityType': data['entityType'],
        'sic': data['sic'],
        'sicDescription': data['sicDescription'],
        'insiderTransactionForOwnerExists': data['insiderTransactionForOwnerExists'],
        'insiderTransactionForIssuerExists': data['insiderTransactionForIssuerExists'],
        'name': data['name'],
        'street1': data['addresses']['business']['street1'],
        'street2': data['addresses']['business']['street2'],
        'city': data['addresses']['business']['city'],
        'stateOrCountry': data['addresses']['business']['stateOrCountry'],
        'zipCode': data['addresses']['business']['zipCode'],
        'stateOrCountryDescription': data['addresses']['business']['stateOrCountryDescription']
    }
    return flattened_data

# Define function to load data from Amazon S3 into Amazon RDS
def load_into_rds(bucket_name, aws_access_key_id, aws_secret_access_key, rds_database_url, table_name):
    """Loads flattened data from S3 into RDS."""
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        for obj in s3.list_objects(Bucket=bucket_name)['Contents']:
            if obj['Key'].endswith('.json'):
                file_name = obj['Key']
                response = s3.get_object(Bucket=bucket_name, Key=file_name)
                data = json.loads(response['Body'].read().decode('utf-8'))
                flattened_data = flatten_json(data, file_name)
                df = pd.DataFrame([flattened_data])  # Convert flattened data to DataFrame
                engine = create_engine(rds_database_url)
                df.to_sql(table_name, engine, if_exists='append', index=False)
        return True
    except Exception as e:
        print(f"Error loading data into RDS: {e}")
        return False


# Define input parameters
bucket_name = "sdas1"
aws_access_key_id = "AKIAU6GDX6EDEF7CY2AW"
aws_secret_access_key = "Ot8NfqeYDfdgdrhgsdfhnftgnsftyjOM9uhA8D"
rds_database_url = f"mysql+pymysql://admin:80016745@database-1.czy8qaeuq6nt.ap-southeast-2.rds.amazonaws.com:3306" 
table_name = "new_data"

# Call this function to load data from S3 into RDS
load_into_rds(bucket_name, aws_access_key_id, aws_secret_access_key, rds_database_url, table_name)
