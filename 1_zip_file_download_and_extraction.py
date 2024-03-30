import requests
from zipfile import ZipFile
import os

destination_dir = "Dictionary_files"
url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
zip_file_path = os.path.join(destination_dir, "submissions.zip")


# Define function to download zip file from URL
def download_zip_file(url, zip_file_path):
    """Downloads a zip file from the given URL and saves it locally."""
    # Create the destination directory if it doesn't exist
    os.makedirs(os.path.dirname(zip_file_path), exist_ok=True)
    try:
        header = {
            'User-Agent': 'Microsoft Edge/122.0.2365.92 (Official build) (64-bit)'}  # Add a User-Agent header to mimic a browser request
        response = requests.get(url, headers=header, stream=True)
        response.raise_for_status()  # Raise error for non-2xx status codes
        with open(zip_file_path, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Error downloading zip file: {e}")
        return False


# Define function to extract JSON files from the zip file
def extract_json_files(zip_file_path, extraction_dir):
    """Extracts JSON files from the zip file."""
    try:
        with ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extraction_dir)
        return True
    except Exception as e:
        print(f"Error extracting JSON files: {e}")
        return False


# Main program flow
if __name__ == "__main__":
    # Download zip file
    if download_zip_file(url, zip_file_path):
        # Extract JSON files
        if extract_json_files(zip_file_path, destination_dir):
            print("Data extraction completed successfully.")
        else:
            print("Error extracting JSON files.")
    else:
        print("Error downloading zip file.")
