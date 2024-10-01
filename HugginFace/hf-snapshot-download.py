import os
import boto3
import logging
import datetime
from huggingface_hub import snapshot_download
from multiprocessing import Pool, cpu_count, current_process
from pymongo import MongoClient

# Environment setup
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name = 'ap-south-1'
bucket_name = 'voice-annotation-testing'
s3_key_prefix = 'Compiled-Datasets-for-PT-June-2024'

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Initialize MongoDB client
mongo_client = MongoClient('mongodb://krutric_chat_db_rw:Yr3qfGkUwnesXBJ@10.33.100.198:27017,10.33.100.218:27017,10.33.100.60:27017/krutrim-chat?authSource=admin')
db = mongo_client['krutrim-chat']
collection = db['download_details']

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def log_start(details, dataset_source):
    record = {
        'dataset': details['dataset'],
        'type': 'snapshot',
        'source': dataset_source,
        'target_size': 'unknown',
        's3_folder': details['s3_folder'],
        'status': 'started',
        'start_time': datetime.datetime.now(),
        'end_time': None,
        'file_urls': [],
        'file_details': []
    }
    result = collection.insert_one(record)
    logger.info(f"Started processing snapshot: {details['dataset']} from source: {dataset_source}")
    return result.inserted_id

def log_file_start(record_id, file_url):
    file_record = {
        'file_url': file_url,
        'downloaded_file_size': None,
        's3_path': None,
        'status': 'started',
        'error_message': None,
        'start_time': datetime.datetime.now(),
        'end_time': None
    }
    result = collection.update_one(
        {'_id': record_id},
        {'$push': {'file_details': file_record}}
    )
    logger.info(f"Started processing file: {file_url}")
    return result.upserted_id

def log_file_update(record_id, file_url, status, downloaded_file_size, s3_path, error_message, start_time, end_time):
    update_fields = {
        'file_details.$.status': status,
        'file_details.$.end_time': end_time,
        'file_details.$.downloaded_file_size': downloaded_file_size,
        'file_details.$.s3_path': s3_path,
        'file_details.$.error_message': error_message,
        'file_details.$.start_time': start_time,
    }
    collection.update_one(
        {'_id': record_id, 'file_details.file_url': file_url},
        {'$set': update_fields}
    )
    logger.info(f"Updated file status for: {file_url} - Status: {status}")

def log_update(record_id, status, error_message=None):
    update_fields = {
        'status': status,
        'end_time': datetime.datetime.now()
    }
    if error_message is not None:
        update_fields['error_message'] = error_message

    collection.update_one({'_id': record_id}, {'$set': update_fields})
    logger.info(f"Updated snapshot status to: {status}")

def list_s3_files(bucket, prefix):
    s3_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                s3_files.append(obj['Key'])
    return s3_files

def process_file(record_id, local_file_path, s3_key):
    try:
        # Upload file to S3
        s3_client.upload_file(local_file_path, bucket_name, s3_key)

        # Log file update
        log_file_update(record_id, local_file_path, 'completed', os.path.getsize(local_file_path), s3_key, None, datetime.datetime.now(), datetime.datetime.now())
        logger.info(f"Uploaded file {local_file_path} to S3 at {s3_key}")
    except Exception as e:
        # Handle upload failure
        error_message = str(e)
        log_file_update(record_id, local_file_path, 'failed', None, None, error_message, datetime.datetime.now(), datetime.datetime.now())
        logger.error(f"Error uploading file {local_file_path} to S3: {error_message}")

def process_snapshot(snapshot_name, dataset_source):
    core_id = current_process().name
    logger.info(f"Process {core_id} is handling snapshot: {snapshot_name}")
    
    details = {
        'dataset': snapshot_name,
        's3_folder': os.path.join(s3_key_prefix, dataset_source)
    }
    record_id = log_start(details, dataset_source)
    try:
        logger.info(f"Starting download for snapshot: {snapshot_name} by {core_id}")

        # List existing S3 files
        dataset_folder = os.path.join(s3_key_prefix, dataset_source, snapshot_name)
        existing_s3_files = list_s3_files(bucket_name, dataset_folder)

        # Download snapshot
        local_path = snapshot_download(
            dataset_source,
            repo_type="dataset",
            allow_patterns=f"data/{snapshot_name}/*"
        )
        logger.info(f"Download complete for snapshot: {snapshot_name} by {core_id}")

        # Process files
        for root, _, files in os.walk(local_path):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = os.path.join(dataset_folder, file)
                if s3_key not in existing_s3_files:
                    process_file(record_id, file_path, s3_key)
                else:
                    logger.info(f"File {s3_key} already exists in S3. Skipping download and upload.")

        log_update(record_id, 'completed')
        logger.info(f"Snapshot {snapshot_name} processing completed by {core_id}.")
    except Exception as e:
        log_update(record_id, 'failed', str(e))
        logger.error(f"Error processing snapshot {snapshot_name} by {core_id}: {e}")

def main():
    dataset_source = "ArmelR/the-pile-splitted"
    snapshots = [
        "ArXiv",
        "BookCorpus2",
        "Books3",
        "DM Mathematics",
        "Enron Emails",
        "EuroParl",
        "FreeLaw",
        "Github",
        "Gutenberg (PG-19)",
        "HackerNews",
        "NIH ExPorter",
        "OpenSubtitles",
        "OpenWebText2",
        "PhilPapers",
        "Pile-CC",
        "PubMed Abstracts",
        "PubMed Central",
        "StackExchange",
        "UPSTO Backgrounds",
        "Ubuntu IRC",
        "Wikipedia (en)"
    ]

    logger.info("Starting data download and upload process")
    
    with Pool(cpu_count()) as pool:
        pool.starmap(process_snapshot, [(snapshot, dataset_source) for snapshot in snapshots])

    logger.info("Data download and upload finished")

if __name__ == '__main__':
    main()

