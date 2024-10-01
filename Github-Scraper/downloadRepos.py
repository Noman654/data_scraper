

import os
import csv
import logging
import subprocess
import yaml
import shutil
import zipfile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import NoCredentialsError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_repos.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_file='config.yaml'):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def create_output_dir(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")

def read_repositories(csv_file):
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        return [row[0] for row in csv_reader]

def compress_directory(directory_path, zip_file_path):
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory_path):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), directory_path))

def set_aws_credentials(access_key, secret_key, region='us-east-1'):
    os.environ['AWS_ACCESS_KEY_ID'] = access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
    os.environ['AWS_DEFAULT_REGION'] = region

def download_repo(repo, output_dir, access_key, secret_key, region='us-east-1', s3_bucket='ADD_YOUR_BUCKET_NAME', s3_prefix='ADD_YOUR_PREFIX'):
    file_name = repo.split("/")[-1]
    repo_path = os.path.join(output_dir, file_name)
    zip_file_path = os.path.join(output_dir, f"{file_name}.zip")
    
    if not os.path.exists(repo_path):
        try:
            subprocess.run(
                ['git', 'clone', '--depth', '1', '--single-branch', f'https://github.com/{repo}', repo_path],
                check=True
            )
            logger.info(f"Successfully downloaded {repo}")

            # Compress repository directory into a ZIP file
            compress_directory(repo_path, zip_file_path)
            logger.info(f"Successfully compressed {repo_path} to {zip_file_path}")

            # Set AWS credentials programmatically
            set_aws_credentials(access_key, secret_key, region)
            
            # Upload ZIP file to S3
            s3_client = boto3.client('s3')
            s3_key = os.path.join(s3_prefix, f"{file_name}.zip")
            s3_client.upload_file(zip_file_path, s3_bucket, s3_key)
            logger.info(f"Uploaded {zip_file_path} to S3://{s3_bucket}/{s3_key}")

            # Clean up local files after upload
            os.remove(zip_file_path)
            shutil.rmtree(repo_path)
            logger.info(f"Deleted local files {zip_file_path} and {repo_path}")

        except subprocess.CalledProcessError as e:
            logger.error(f"Error downloading {repo}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
    else:
        logger.info(f"Already downloaded {repo}")

def main():
    config = load_config()
    csv_file = config['csv_file']
    output_dir = config['output_dir']
    num_jobs = config['num_jobs']
    access_key = config['aws_access_key']
    secret_key = config['aws_secret_key']
    region = config.get('aws_region', 'us-east-1')

    create_output_dir(output_dir)
    repo_names = read_repositories(csv_file)
    
    logger.info("Starting the download process")
    
    # Using ThreadPoolExecutor for multithreading
    with ThreadPoolExecutor(max_workers=num_jobs) as executor:
        futures = []
        for name in repo_names:
            futures.append(executor.submit(download_repo, name, output_dir, access_key, secret_key, region))
        
        # Wait for all tasks to complete
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Failed to download repo: {e}")

    logger.info("Download process completed")

if __name__ == "__main__":
    main()
