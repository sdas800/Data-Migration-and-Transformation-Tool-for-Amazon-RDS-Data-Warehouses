import boto3
import json
import pandas as pd
from sqlalchemy import create_engine

# Define function to load data from Amazon S3 into Amazon RDS
def load_into_rds(bucket_name, aws_access_key_id, aws_secret_access_key, rds_database_url, table_name):
    """Loads data from S3 into RDS."""
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        for obj in s3.list_objects(Bucket=bucket_name)['Contents']:
            if obj['Key'].endswith('.json'):
                file_name = obj['Key']
                response = s3.get_object(Bucket=bucket_name, Key=file_name)
                data = json.loads(response['Body'].read().decode('utf-8'))
                df = pd.DataFrame(data)
                # Select only the first 5 rows and 5 columns
                df = df.head(5).iloc[:, :5]
                engine = create_engine(rds_database_url)
                df.to_sql(table_name, engine, if_exists='append', index=False)
        return True
    except Exception as e:
        print(f"Error loading data into RDS: {e}")
        return False


# Define input parameters
bucket_name = "YOUR_S3_BUCKET_NAME"
aws_access_key_id = "YOUR_AWS_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_AWS_SECRET_ACCESS_KEY"
rds_database_url = "RDS_DATABASE_URL"
table_name = "YOUR_TABLE_NAME"

# Call this function to load data from S3 into RDS
load_into_rds(bucket_name, aws_access_key_id, aws_secret_access_key, rds_database_url, table_name)
