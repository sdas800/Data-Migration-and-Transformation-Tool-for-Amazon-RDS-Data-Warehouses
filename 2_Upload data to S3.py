import boto3
import os

# Define function to upload JSON files to Amazon S3
def upload_to_s3(json_files_directory, bucket_name, prefix=''):
    """Uploads JSON files to the specified S3 bucket."""
    s3 = boto3.client('s3')
    try:
        for file_name in os.listdir(json_files_directory):
            file_path = os.path.join(json_files_directory, file_name)
            s3.upload_file(file_path, bucket_name, f"{prefix}{file_name}")
        return True
    except Exception as e:
        print(f"Error uploading files to S3: {e}")
        return False

# Main program flow
if __name__ == "__main__":
    # Define input parameters
    json_files_directory = "Dictionary_files"
    bucket_name = "YOUR_S3_BUCKET_NAME"
    prefix = "OPTIONAL_PREFIX_FOR_S3_KEYS"

    # Upload JSON files to S3
    if upload_to_s3(json_files_directory, bucket_name, prefix):
        print("Data successfully uploaded to S3.")
    else:
        print("Error uploading files to S3.")