import os
import multiprocessing
# import s3fs
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import huggingface_hub
import requests
import boto3
from botocore.config import Config
import logging

from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


import datetime

# Set your AWS credentials using environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = 'voice-annotation-testing'
s3_key_prefix = 'Compiled-Datasets-for-PT-Aug-2024/Mint1T'

# Create an S3 filesystem object
# s3 = s3fs.S3FileSystem(
#     anon=False,
#     key=aws_access_key_id,
#     secret=aws_secret_access_key
# )

# List all files in the repository, including their path


REPO_ID =None
repo_type = "dataset"

config = Config(
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    },
    max_pool_connections=50  # Increase the pool size
)
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
config = config
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def check_s3_file_exists(bucket_name, file_key):
    """
    Check if a file exists in an S3 bucket.

    :param bucket_name: Name of the S3 bucket.
    :param file_key: Key (path) of the file in the S3 bucket.
    :return: True if file exists, False otherwise.
    """


    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except ClientError as e:
        # If a 404 error is thrown, then the object does not exist.
        if e.response['Error']['Code'] == '404':
            return False
        # If any other error is thrown, re-raise it.
        raise
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")
        return False
  


def log_file_update(record_id, file_url, status, downloaded_file_size, s3_path, error_message, start_time, end_time):
    update_fields = {
        'file_details.$.status': status,
        'file_details.$.end_time': end_time,
        'file_details.$.downloaded_file_size': downloaded_file_size,
        'file_details.$.s3_path': s3_path,
        'file_details.$.error_message': error_message,
        'file_details.$.start_time': start_time,
    }
    # collection.update_one(
    #     {'_id': record_id, 'file_details.file_url': file_url},
    #     {'$set': update_fields}
    # )
    logger.info(f"Updated file status for: {file_url} - Status: {status}")

def process_file_s3(record_id, local_file_path, s3_key):
    try:
        record_id =None
        # Upload file to S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,

        )
        s3_client.upload_file(local_file_path, bucket_name, s3_key)

        # Log file update
        log_file_update(record_id, local_file_path, 'completed', os.path.getsize(local_file_path), s3_key, None, datetime.datetime.now(), datetime.datetime.now())
        logger.info(f"Uploaded file {local_file_path} to S3 at {s3_key}")

        print("Deleteing file ", local_file_path)
        os.remove(local_file_path)
    except Exception as e:
        # Handle upload failure
        error_message = str(e)
        log_file_update(record_id, local_file_path, 'failed', None, None, error_message, datetime.datetime.now(), datetime.datetime.now())
        logger.error(f"Error uploading file {local_file_path} to S3: {error_message}")


def download_and_upload_to_s3(file_path):
    """Download a file from Hugging Face and upload it directly to S3."""
    try:
        s3_file_path = f"{s3_key_prefix}/{REPO_ID}/{file_path}"

        if check_s3_file_exists(bucket_name=bucket_name, file_key=s3_file_path):
            print("file is Exist skip", s3_file_path)
            return "", None

        file_url = huggingface_hub.hf_hub_url(repo_id=REPO_ID, filename=file_path, repo_type=repo_type)
        response = requests.get(file_url, stream=True)
        response.raise_for_status()

        # print(s3_file_path)
        # s3_client.upload_fileobj(response.raw, bucket_name, s3_file_path)

        
        try:
            # Stream the file directly to S3 with progress
            s3_client.upload_fileobj(response.raw, bucket_name, s3_file_path)
            print("Completed", s3_file_path)
            
        except Exception as upload_error:
            print(f"Upload failed for {file_path}: {upload_error}")

            # If upload failed, delete the incomplete file from S3
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=s3_file_path)
                print(f"Deleted incomplete file from S3: {s3_file_path}")
            except Exception as delete_error:
                print(f"Failed to delete incomplete file from S3: {delete_error}")

    except Exception as e:
        return file_path, str(e)  # Return the file path and error if any
    
    finally:
        return "", None

def process_files(file_list):
    """Process a list of files using threading within this process."""
    max_workers = 35   # Adjust based on your system and network capacity
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(download_and_upload_to_s3, file): file for file in file_list}
        
        for future in tqdm(as_completed(future_to_file), total=len(file_list), desc="Processing files"):
            file_path, error = future.result()
            if error:
                print(f"Failed to download {file_path}: {error}")
            else:
                results.append(file_path)
                print(f"Successfully downloaded and uploaded {file_path}")

    return results

def main():
    
    # get the dataset list
    datasetst_f = [
            # "mlfoundations/MINT-1T-PDF-CC-2024-10",
            # "mlfoundations/MINT-1T-PDF-CC-2023-50",
            # "mlfoundations/MINT-1T-PDF-CC-2023-40",
            # "mlfoundations/MINT-1T-PDF-CC-2023-23",
            "mlfoundations/MINT-1T-PDF-CC-2023-14",
            "mlfoundations/MINT-1T-PDF-CC-2023-06"
            ]

    for dataset in datasetst_f:
        global REPO_ID
        REPO_ID =dataset

        
        # get the list of files
        files_list = huggingface_hub.list_repo_files(repo_id=REPO_ID, repo_type=repo_type)

        # Get the number of CPUs
        num_cpus = os.cpu_count()

        print(REPO_ID, len(files_list))
        
        # Split the file list into chunks, one for each CPU
        chunk_size = len(files_list) // num_cpus
        file_chunks = [files_list[i:i + chunk_size] for i in range(0, len(files_list), chunk_size)]
        
        # If there are remaining files, add them to the last chunk
        if len(files_list) % num_cpus != 0:
            file_chunks[-1].extend(files_list[num_cpus * chunk_size:])
        
        # Create a pool of processes
        with multiprocessing.Pool(processes=num_cpus) as pool:
            pool.map(process_files, file_chunks)

if __name__ == "__main__":
    main()



