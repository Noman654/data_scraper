import os
import logging
from botocore.exceptions import NoCredentialsError
from multiprocessing import Pool, cpu_count
from pymongo import MongoClient
from datetime import datetime
import aiohttp
import asyncio
import aioboto3
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS S3 configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'voice-annotation-testing'  # Ensure this environment variable is set

# MongoDB configuration
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://krutric_chat_db_rw:Yr3qfGkUwnesXBJ@10.33.100.198:27017,10.33.100.218:27017,10.33.100.60:27017/krutrim-chat?authSource=admin')
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['krutrim-chat']
collection = db['sanskrit_data_download']

async def download_file(session, url, output_folder, item_name):
    local_filename = url.split('/')[-1][:255]  # Limit file name length
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    local_path = os.path.join(output_folder, local_filename)

    retry_attempts = 3
    for attempt in range(retry_attempts):
        try:
            async with session.get(url, timeout=300) as response:  # Set a timeout directly on the session
                response.raise_for_status()
                with open(local_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
            logging.info(f'Downloaded: {local_filename}')
            return local_path
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logging.warning(f'Attempt {attempt + 1} failed for {url} with error: {e}')
            if attempt == retry_attempts - 1:
                logging.error(f'Failed to download {url} after {retry_attempts} attempts')
                # Fallback to requests
                try:
                    response = requests.get(url, stream=True, timeout=300)
                    response.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(8192):
                            f.write(chunk)
                    logging.info(f'Successfully downloaded with requests: {local_filename}')
                    return local_path
                except (requests.RequestException, Exception) as e:
                    logging.error(f'Failed to download {url} with requests: {e}')
                    # Update MongoDB with failed status
                    collection.update_one(
                        {'folder_name': item_name},
                        {'$set': {f'download_status.{url}': 'failed'}},
                        upsert=True
                    )
                    return None
        await asyncio.sleep(2)  # Wait before retrying

async def upload_to_s3(file_path, s3_folder):
    file_name = os.path.basename(file_path)
    s3_key = os.path.join(s3_folder, file_name)
    session = aioboto3.Session()
    async with session.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY) as s3_client:
        try:
            await s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
            logging.info(f'Uploaded {file_name} to {s3_key}')
        except FileNotFoundError:
            logging.error(f'The file was not found: {file_path}')
        except NoCredentialsError:
            logging.error('Credentials not available')

async def process_item_async(item_url):
    logging.info(f'Processing {item_url}')
    output_folder = 'downloads'
    item_name = item_url.split('/')[-1]
    s3_prefix = 'sanskrit-data-download'
    s3_folder = os.path.join(s3_prefix, item_name)

    start_time = datetime.utcnow()
    file_paths = []

    async with aiohttp.ClientSession() as session:
        file_path = await download_file(session, item_url, output_folder, item_name)
        if file_path:
            file_paths.append(file_path)
            collection.update_one(
                {'folder_name': item_name},
                {'$set': {f'download_status.{item_url}': 'success'}},
                upsert=True
            )
            await upload_to_s3(file_path, s3_folder)
    
    end_time = datetime.utcnow()
    folder_size = sum(os.path.getsize(f) for f in file_paths if f)
    metadata = {
        'folder_name': item_name,
        'url': item_url,
        's3_path': s3_folder,
        'start_time': start_time,
        'end_time': end_time,
        'folder_size': folder_size,
        'download_links': [item_url],
        'total_files': len(file_paths),
        'failed_links': [item_url] if f'download_status.{item_url}' in collection.find_one({'folder_name': item_name}) and collection.find_one({'folder_name': item_name})[f'download_status.{item_url}'] == 'failed' else []
    }
    
    existing_entry = collection.find_one({'folder_name': item_name})
    if existing_entry is None:
        collection.insert_one(metadata)
        logging.info(f'Metadata saved for {item_name}')
    else:
        collection.update_one({'folder_name': item_name}, {'$set': metadata})
        logging.warning(f'Entry for {item_name} already exists in MongoDB, updated metadata.')

def process_item(item_url):
    asyncio.run(process_item_async(item_url))

# Main script to download multiple files
def main():
    pdf_urls = [
       "https://ia801503.us.archive.org/1/items/in.ernet.dli.2015.369057/2015.369057.Naushhadhiiyacharitamuu.pdf"
       
    ]

    num_processes = min(len(pdf_urls), cpu_count())
    with Pool(processes=num_processes) as pool:
        pool.map(process_item, pdf_urls)

if __name__ == "__main__":
    main()
