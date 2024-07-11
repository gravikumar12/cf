import re
import tempfile
import pandas as pd
import os
from google.cloud import storage

def remove_special_characters(text):
    # Define regex to match special characters
    special_chars_regex = re.compile(r'[@!#$%^&*()<>?/\|}{~:]')
    # Remove special characters
    return special_chars_regex.sub('', str(text))

def clean_csv(event, context):
    # Extract bucket and file information from the event
    file_name = event['name']
    bucket_name = event['bucket']

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Retrieve the bucket and file from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the file to a temporary location
    temp_dir = tempfile.TemporaryDirectory()
    temp_file_path = os.path.join(temp_dir.name, file_name)
    blob.download_to_filename(temp_file_path)

    # Read CSV file into pandas DataFrame without header
    df = pd.read_csv(temp_file_path, header=None)

    # Clean the CSV data
    df_cleaned = df.applymap(remove_special_characters)

    # Create output directory if not exists
    output_dir = 'output'
    os.makedirs(os.path.join(temp_dir.name, output_dir), exist_ok=True)

    # Write cleaned DataFrame to a new CSV file
    cleaned_file_path = os.path.join(temp_dir.name, output_dir, f"cleaned_{file_name}")
    df_cleaned.to_csv(cleaned_file_path, index=False, header=False)

    # Upload cleaned file back to GCS in the output directory
    cleaned_blob = bucket.blob(os.path.join(output_dir, f"cleaned_{file_name}"))
    cleaned_blob.upload_from_filename(cleaned_file_path)

    # Cleanup: Delete temporary directory and files
    temp_dir.cleanup()

    print(f"Cleaned CSV file stored in bucket: {bucket_name} under {output_dir}/cleaned_{file_name}")
