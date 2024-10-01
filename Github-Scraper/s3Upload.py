## In case some zip files are not uploaded to s3 path from your local, run this script, it will push to s3


import os
import boto3
from botocore.exceptions import NoCredentialsError

# Define your variables
folder_path = ""  # Replace with the path to your local folder
s3_bucket = ""
s3_prefix = ""

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')

def upload_files():
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".zip"):
                local_path = os.path.join(root, file)
                s3_path = os.path.join(s3_prefix, file)

                try:
                    s3.upload_file(local_path, s3_bucket, s3_path)
                    print(f"Uploaded {local_path} to s3://{s3_bucket}/{s3_path}")
                except FileNotFoundError:
                    print(f"The file {local_path} was not found")
                except NoCredentialsError:
                    print("Credentials not available")

if __name__ == "__main__":
    upload_files()
