import requests
from bs4 import BeautifulSoup
import os
import logging
from botocore.exceptions import NoCredentialsError
from multiprocessing import Pool, cpu_count
from pymongo import MongoClient
from datetime import datetime
import aiohttp
import asyncio
import aioboto3
import async_timeout

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

# Function to read URLs from data.txt
def read_urls_from_file(file_path):
    with open(file_path, 'r') as f:
        urls = [line.strip()[6:-7] for line in f.readlines() if line.strip().startswith('<link>https://archive.org/details')]
    return urls

# Function to find download links
def get_download_links(item_url, format='PDF'):
    response = requests.get(item_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    download_links = []
    for a in soup.find_all('a', href=True):
        if format in a.text:
            download_links.append(a['href'])
    return download_links

async def download_file(session, url, output_folder, item_name, download_link):
    local_filename = url.split('/')[-1][:255]  # Limit file name length
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    local_path = os.path.join(output_folder, local_filename)

    retry_attempts = 1
    for attempt in range(retry_attempts):
        try:
            async with session.get(url, timeout=2) as response:  # Set a timeout directly on the session
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
                        {'$set': {f'download_status.{download_link}': 'failed'}},
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
    base_url = "https://archive.org"
    output_folder = 'downloads'
    file_format = 'PDF'
    item_name = item_url.split('/')[-1]
    s3_prefix = 'sanskrit-data-download/Jangamwadi-Math-Library-Collection'
    s3_folder = os.path.join(s3_prefix, item_name)

    start_time = datetime.utcnow()
    download_links = get_download_links(item_url, format=file_format)
    file_paths = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for link in download_links:
            task = download_file(session, base_url + link, output_folder, item_name, link)
            tasks.append(task)
        file_paths = await asyncio.gather(*tasks)

        upload_tasks = []
        for file_path in file_paths:
            if file_path:
                collection.update_one(
                    {'folder_name': item_name},
                    {'$set': {f'download_status.{link}': 'success'}},
                    upsert=True
                )
                upload_task = upload_to_s3(file_path, s3_folder)
                upload_tasks.append(upload_task)
        await asyncio.gather(*upload_tasks)
    
    end_time = datetime.utcnow()
    folder_size = sum(os.path.getsize(f) for f in file_paths if f)
    metadata = {
        'folder_name': item_name,
        'url': item_url,
        's3_path': s3_folder,
        'start_time': start_time,
        'end_time': end_time,
        'folder_size': folder_size,
        'download_links': download_links,
        'total_files': len(file_paths),
        'failed_links': [link for link in download_links if f'download_status.{link}' in collection.find_one({'folder_name': item_name}) and collection.find_one({'folder_name': item_name})[f'download_status.{link}'] == 'failed']
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
    data_file = 'data.txt'
    item_urls = read_urls_from_file(data_file)
    
    num_processes = min(len(item_urls), cpu_count())
    with Pool(processes=num_processes) as pool:
        pool.map(process_item, item_urls)

if __name__ == "__main__":
    main()
