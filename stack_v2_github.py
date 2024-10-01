import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from tqdm import tqdm
import pandas as pd
import subprocess
from smart_open import open
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import gc

# Logging configuration
log_file = 'processing.log'
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

s3_client = boto3.client('s3')
bucket_name = 'llm-spark'
input_prefix = 'Stack_V2/'
local_temp_dir = '/tmp'
output_prefix = "github_download/v2"

def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        s3_dest = f's3://{bucket_name}/{s3_key}'
        subprocess.run(['aws', 's3', 'cp', file_path, s3_dest], check=True)
        logging.info(f"Uploaded {file_path} to {s3_dest}")
        os.remove(file_path)
        logging.info(f"Deleted local file {file_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to upload {file_path} to S3: {e}")

def download_contents(blob_id, src_encoding):
    s3_url = f"s3://softwareheritage/content/{blob_id}"
    try:
        with open(s3_url, "rb", compression=".gz") as fin:
            content = fin.read().decode(src_encoding)
        return {"text": content, "word_count": len(content.split())}
    except Exception as e:
        logging.error(f"Error downloading blob_id {blob_id}: {e}")
        return {"text": "", "word_count": 0}

def process_row(row):
    content_data = download_contents(row["blob_id"], row["src_encoding"])
    row.update(content_data)
    return row

def process_file(local_file_path, s3_key):
    try:
        parquet_file = pq.ParquetFile(local_file_path)
        updated_rows = []
        
        for batch in tqdm(parquet_file.iter_batches(batch_size=1000), desc=f"Processing {s3_key}", unit="batch"):
            df = batch.to_pandas()
            rows = df.to_dict('records')
            
            with Pool(cpu_count()) as pool:
                processed_rows = list(tqdm(pool.imap(process_row, rows), desc=f"Processing rows in {s3_key}", total=len(rows), unit="row"))
            
            updated_rows.extend(processed_rows)
        
        updated_table = pa.Table.from_pandas(pd.DataFrame(updated_rows))
        pq.write_table(updated_table, local_file_path, compression='snappy')
        
        if os.path.exists(local_file_path):
            logging.info(f"Output file {local_file_path} exists, proceeding to upload.")
            s3_output_key = f'{output_prefix}/{os.path.basename(s3_key)}'
            upload_to_s3(local_file_path, bucket_name, s3_output_key)
        else:
            logging.error(f"Output file {local_file_path} does not exist, skipping upload.")
        
        del updated_rows, updated_table, parquet_file
        gc.collect()
        
    except Exception as e:
        logging.error(f"Failed to process {s3_key}: {e}")

def resumable_download(bucket, key, local_path):
    if os.path.exists(local_path):
        existing_size = os.path.getsize(local_path)
    else:
        existing_size = 0
    
    response = s3_client.head_object(Bucket=bucket, Key=key)
    total_size = response['ContentLength']
    
    if existing_size == total_size:
        logging.info(f"{local_path} is already fully downloaded.")
        return
    
    with open(local_path, 'ab') as f:
        for start in tqdm(range(existing_size, total_size, 1024 * 1024), desc=f"Downloading {key}", unit='B', unit_scale=True, unit_divisor=1024):
            end = min(start + 1024 * 1024 - 1, total_size - 1)
            response = s3_client.get_object(Bucket=bucket, Key=key, Range=f'bytes={start}-{end}')
            f.write(response['Body'].read())
            logging.info(f"Downloaded bytes {start}-{end} of {key}")

def list_s3_files(bucket_name, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    file_keys = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    file_keys.append(obj['Key'])
    return file_keys

def download_files(file_keys):
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        futures = []
        for s3_key in file_keys:
            local_file_path = f'{local_temp_dir}/{os.path.basename(s3_key)}'
            futures.append(executor.submit(resumable_download, bucket_name, s3_key, local_file_path))
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading files"):
            future.result()

def list_processed_files():
    return list_s3_files(bucket_name, output_prefix)

def process_files_from_json(json_file_path):
    with open(json_file_path, 'r') as json_file:
        file_keys = json.load(json_file)

    # Get a list of processed files
    processed_files = list_processed_files()
    processed_files_set = set(os.path.basename(key) for key in processed_files)

    # Filter out already processed files
    file_keys_to_process = [key for key in file_keys if os.path.basename(key) not in processed_files_set]
    
    # Log the number of files to be processed
    logging.info(f"{len(file_keys_to_process)} out of {len(file_keys)} files to process.")
    
    # Download files concurrently
    download_files(file_keys_to_process)
    
    # Process files sequentially to limit memory usage
    for s3_key in tqdm(file_keys_to_process, desc="Processing files from JSON"):
        local_file_path = f'{local_temp_dir}/{os.path.basename(s3_key)}'
        process_file(local_file_path, s3_key)
        gc.collect()  # Collect garbage after processing each file to free up memory

if __name__ == "__main__":
    json_file_path = 'batch1.json'  # Update this with the path to your JSON file
    process_files_from_json(json_file_path)
    logging.info("Processing complete.")
