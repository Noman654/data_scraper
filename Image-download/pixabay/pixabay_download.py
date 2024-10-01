
import re
import os
import json
import logging
import requests
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import time
from pymongo import MongoClient
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("scraping.log"),
                        logging.StreamHandler()
                    ])

# MongoDB configuration
MONGO_CONNECTION_STRING = 'mongodb://krutric_chat_db_rw:Yr3qfGkUwnesXBJ@10.33.100.198:27017,10.33.100.218:27017,10.33.100.60:27017/krutrim-chat?authSource=admin'
MONGO_DB_NAME = 'krutrim-chat'
MONGO_COLLECTION_NAME = 'image-download_details'

# AWS S3 configuration
S3_BUCKET_NAME = 'voice-annotation-testing'
S3_PREFIX = 'Audio-video-image-dataset/images-june-download/pixabay/'  # Prefix where you want to upload files

def connect_to_mongodb():
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    return collection

def store_image_metadata(collection, item, links, alts):
    try:
        data = {'item': item, 'links': links, 'alts': alts}
        collection.insert_one(data)
        logging.info(f"Stored metadata for '{item}' in MongoDB.")
    except Exception as e:
        logging.error(f"Failed to store metadata in MongoDB for '{item}': {e}")

def scroll_down(driver):
    logging.info("Scrolling down the page.")
    page_height = driver.execute_script("return document.body.scrollHeight")
    total_scrolled = 0
    step_size = 500  # Adjust the step size if necessary
    while total_scrolled < page_height:
        driver.execute_script(f'window.scrollBy(0, {step_size});')
        time.sleep(1)
        total_scrolled += step_size
        page_height = driver.execute_script("return document.body.scrollHeight")
    logging.info("Scroll down complete.")

def sanitize_filename(name):
    # Remove any character that is not alphanumeric or a space, replace spaces with underscores
    return re.sub(r'[^a-zA-Z0-9\s]', '', name).replace(' ', '_')

def download_image(url, folder_path, image_title):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            image_name = sanitize_filename(image_title) + '.jpg'
            image_path = os.path.join(folder_path, image_name)
            with open(image_path, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            logging.info(f"Downloaded image: {image_path}")
            return image_path  # Return the local file path for S3 upload
        else:
            logging.warning(f"Failed to download image from {url}")
            return None
    except Exception as e:
        logging.error(f"Error downloading image from {url}: {e}")
        return None

def upload_image_to_s3(local_image_path, s3_bucket, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_image_path, s3_bucket, s3_key)
        logging.info(f"Uploaded {local_image_path} to S3://{s3_bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload {local_image_path} to S3: {e}")

def check_new_content(driver, current_content):
    soup = BeautifulSoup(driver.page_source, 'lxml')
    new_content = [img['src'] for img in soup.find_all('img') if 'src' in img.attrs]
    return new_content != current_content, new_content

def imagescrape(driver, item, mongodb_collection):
    output_dir = './Pixabay'
    item_dir = os.path.join(output_dir, item.replace(' ', '_'))

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
        logging.info(f"Created output directory at {output_dir}")

    if not os.path.exists(item_dir):
        os.mkdir(item_dir)
        logging.info(f"Created item directory at {item_dir}")

    base_url = f'https://pixabay.com/images/search/{item}/'
    links = []
    alts = []
    page_number = 1
    downloaded_count = 0  # Track number of downloaded images
    batch_size = 100  # Upload to S3 after this many images
    current_content = []

    try:
        with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
            while True:
                url = base_url + '?pagi=' + str(page_number)
                logging.info(f"Accessing URL: {url}")
                driver.get(url)
                page_number += 1
                scroll_down(driver)
                
                soup = BeautifulSoup(driver.page_source, 'lxml')

                no_results = soup.find_all(string=re.compile('^Try another search term$'))
                if no_results:
                    logging.warning("No results found.")
                    break

                articles = soup.findAll('a', attrs={'class': 'link--WHWzm'})
                if not articles:
                    logging.warning("No images found on this page.")
                    break

                has_new_content, current_content = check_new_content(driver, current_content)
                if not has_new_content:
                    logging.info("No new content found. Ending scraping for this keyword.")
                    break

                futures = []
                for article in articles:
                    img_tag = article.find('img')
                    if img_tag:
                        img_src = img_tag['src']
                        img_alt = img_tag['alt']
                        img_src = urljoin(base_url, img_src)
                        if img_src in links:
                            continue
                        links.append(img_src)
                        alts.append(img_alt)
                        # Submit download task to executor
                        futures.append(executor.submit(download_image, img_src, item_dir, img_alt))

                # Wait for all futures to complete
                for future in as_completed(futures):
                    local_image_path = future.result()
                    if local_image_path:
                        downloaded_count += 1
                        if downloaded_count % batch_size == 0:
                            upload_batch_to_s3(item, item_dir, S3_BUCKET_NAME, S3_PREFIX)
                            delete_batch_from_local(item_dir, batch_size)

            if links:
                store_image_metadata(mongodb_collection, item, links, alts)
                # Upload remaining images and delete local folder
                upload_batch_to_s3(item, item_dir, S3_BUCKET_NAME, S3_PREFIX)
                delete_batch_from_local(item_dir, batch_size)

                logging.info(f"Data saved for {item}")
            else:
                logging.warning("No data to save.")

    except Exception as e:
        logging.error(f"An error occurred while scraping for item '{item}': {e}")

def upload_batch_to_s3(item, local_folder_path, s3_bucket, s3_prefix):
    s3 = boto3.client('s3')
    item_key = f"{s3_prefix}{item}/"
    for dirpath, _, filenames in os.walk(local_folder_path):
        for filename in filenames:
            local_file_path = os.path.join(dirpath, filename)
            s3_object_key = item_key + filename
            try:
                s3.upload_file(local_file_path, s3_bucket, s3_object_key)
                logging.info(f"Uploaded {local_file_path} to S3://{s3_bucket}/{s3_object_key}")
            except Exception as e:
                logging.error(f"Failed to upload {local_file_path} to S3: {e}")

def delete_batch_from_local(local_folder_path, batch_size):
    try:
        count = 0
        for root, dirs, files in os.walk(local_folder_path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
                count += 1
                if count >= batch_size:
                    break
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
                count += 1
                if count >= batch_size:
                    break
            if count >= batch_size:
                break
        logging.info(f"Deleted {batch_size} files from local folder: {local_folder_path}")
    except Exception as e:
        logging.error(f"Failed to delete files from local folder {local_folder_path}: {e}")

def main():
    try:
        with open("data.json", "r") as file:
            keywords = json.load(file)

        options = Options()
        options.headless = True  # Run Chrome in headless mode
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)

        logging.info("Scraping started.")
        mongodb_collection = connect_to_mongodb()
        for item in tqdm(keywords, desc='SCRAPING'):
            item = item.strip()
            logging.info(f"Started scraping for: {item}")
            try:
                imagescrape(driver, item, mongodb_collection)
                logging.info(f"Completed scraping for: {item}")
            except Exception as e:
                logging.error(f"An error occurred while scraping for '{item}': {e}")
        logging.info("Scraping completed.")

    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

    finally:
        driver.quit()
        logging.info("WebDriver closed.")

if __name__ == '__main__':
    main()
