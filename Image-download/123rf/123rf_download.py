import os
import time
import requests
import boto3
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
from time import sleep
from random import randint

# AWS S3 setup
s3 = boto3.client('s3')
BUCKET_NAME = 'voice-annotation-testing'  # Correct bucket name

# MongoDB setup
# client = MongoClient('localhost', 27017)  # Adjust the host and port if needed
# db = client['image_metadata_db']
# collection = db['image_metadata']
mongo_client = MongoClient('mongodb://krutric_chat_db_rw:Yr3qfGkUwnesXBJ@10.33.100.198:27017,10.33.100.218:27017,10.33.100.60:27017/krutrim-chat?authSource=admin')
db = mongo_client['krutrim-chat']
collection = db['image-download_details']



# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraping_log.log"),
        logging.StreamHandler()
    ]
)

# Function to store image metadata in MongoDB
def store_image_metadata(folder_name, img_url, s3_key, img_name, alt_text):
    metadata = {
        'img_url': img_url,
        's3_key': s3_key,
        'img_name': img_name,
        'alt_text': alt_text
    }
    try:
        collection.update_one(
            {'folder_name': folder_name},
            {'$push': {'images': metadata}},
            upsert=True
        )
        logging.info(f"Stored metadata for image: {img_name}")
    except Exception as e:
        logging.error(f"Failed to store metadata for {img_name}: {e}")

# Function to download image with retry mechanism
def download_image(img_info, folder_name):
    img_url, img_name, image_dir = img_info
    retries = 5
    for i in range(retries):
        try:
            with requests.Session() as session:
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                })
                response = session.get(img_url, stream=True, timeout=10)
                if response.status_code == 403:
                    logging.error(f"Received 403 error for {img_url}. Rate limit exceeded.")
                    sleep(300)  # Wait for 5 minutes before retrying
                    continue
                response.raise_for_status()  # Raise an error for bad status codes
                with open(os.path.join(image_dir, img_name), 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
            logging.info(f"Downloaded image: {img_name}")

            # Store metadata in MongoDB
            s3_key = f"Audio-video-image-dataset/images-june-download/123rf/{folder_name}/{img_name}"
            store_image_metadata(folder_name, img_url, s3_key, img_name, os.path.splitext(img_name)[0])
            break  # Exit the retry loop if successful
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading {img_url}: {e}")
            sleep(2 ** i + randint(0, 1000) / 1000)  # Exponential backoff
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            break

# Function to upload images to S3 and delete them locally
def upload_images_to_s3_and_delete_local(image_dir, folder_name):
    for img_name in os.listdir(image_dir):
        img_path = os.path.join(image_dir, img_name)
        s3_key = f"Audio-video-image-dataset/images-june-download/123rf/{folder_name}/{img_name}"  # Correct key
        try:
            s3.upload_file(img_path, BUCKET_NAME, s3_key)
            os.remove(img_path)
            logging.info(f"Uploaded and removed {img_name} from local storage.")
        except Exception as e:
            logging.error(f"Failed to upload {img_name} to S3: {e}")

# Function to get S3 folder size and image count
def get_s3_folder_stats(folder_name):
    total_size = 0
    total_count = 0
    prefix = f"Audio-video-image-dataset/images-june-download/123rf/{folder_name}/"
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            total_size += obj['Size']
            total_count += 1
    
    return total_size, total_count

# Function to process a single URL
def process_url(folder_name, url):
    logging.info(f"Processing folder: {folder_name} with URL: {url}")

    start_time = datetime.now()

    # Record start time in MongoDB
    try:
        collection.update_one(
            {'folder_name': folder_name},
            {'$set': {'start_time': start_time}},
            upsert=True
        )
        logging.info(f"Recorded start time for folder: {folder_name}")
    except Exception as e:
        logging.error(f"Failed to record start time for folder {folder_name}: {e}")

    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = Service('/home/mdalam8/beautifulsetup/chromedriver-linux64/chromedriver')  # Update path to your chromedriver
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Open the URL
    driver.get(url)
    time.sleep(5)

    # Create a specific directory for the current link if it doesn't exist
    image_dir = os.path.join("images", folder_name)
    if not os.path.exists(image_dir):
        os.makedirs(image_dir)

    image_count = 0

    while True:
        # Parse the page source with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find all image tags with the specified class
        images = soup.find_all('img', class_='ImageThumbnail__image ImageThumbnail__image--beta')

        logging.info(f"Found {len(images)} images on the page.")

        img_info_list = []
        for img in images:
            img_url = img.get('src') or img.get('data-src')
            if img_url and img_url.startswith('http'):
                alt_text = img.get('alt', '').replace(' ', '_').replace('/', '_')
                img_name = f"image_{image_count}_{alt_text}.jpg"
                img_info_list.append((img_url, img_name, image_dir))
                image_count += 1
            else:
                logging.warning(f"Skipping invalid URL: {img_url}")
        
        # Download images sequentially
        for img_info in img_info_list:
            download_image(img_info, folder_name)

        # Upload images to S3 and delete locally after every 100 images
        if image_count % 100 == 0:
            logging.info(f"Uploading batch of 100 images to S3 for folder: {folder_name}")
            upload_images_to_s3_and_delete_local(image_dir, folder_name)

        # Check for next page button and click if available
        try:
            next_button = driver.find_element(By.ID, 'imagesearch-btn-nextpg')
            if next_button:
                next_button.click()
                time.sleep(5)  # Wait for the next page to load
            else:
                break
        except Exception as e:
            logging.info("No more pages to load or error occurred: " + str(e))
            break

    # Upload remaining images to S3 and delete locally
    logging.info(f"Uploading remaining images to S3 for folder: {folder_name}")
    upload_images_to_s3_and_delete_local(image_dir, folder_name)

    # Quit the driver
    driver.quit()

    end_time = datetime.now()

    # Get total size and count of images from S3
    total_size, total_count = get_s3_folder_stats(folder_name)

    # Store folder metadata
    try:
        collection.update_one(
            {'folder_name': folder_name},
            {'$set': {
                'total_images': total_count,
                'total_size': total_size,
                'end_time': end_time
            }},
            upsert=True
        )
        logging.info(f"Stored metadata for folder: {folder_name}")
    except Exception as e:
        logging.error(f"Failed to store metadata for folder {folder_name}: {e}")

# Main function to handle multiple URLs
def main(urls):
    for folder_name, url in urls:
        process_url(folder_name, url)

if __name__ == "__main__":
   urls = [
    ("india", "https://www.123rf.com/stock-photo/yogi.html"),
    ("india", "https://www.123rf.com/stock-photo/abhinaya.html"),
    ("india", "https://www.123rf.com/stock-photo/angrezi.html"),
    ("india", "https://www.123rf.com/stock-photo/anna.html"),
    ("india", "https://www.123rf.com/stock-photo/ashram.html"),
    ("india", "https://www.123rf.com/stock-photo/azaan.html"),
    ("india", "https://www.123rf.com/stock-photo/Begam.html"),
    ("india", "https://www.123rf.com/stock-photo/Bhagavad_Gita.html"),
    ("india", "https://www.123rf.com/stock-photo/bol.html"),
    ("india", "https://www.123rf.com/stock-photo/Brahma.html"),
    ("india", "https://www.123rf.com/stock-photo/Brahmin.html"),
    ("india", "https://www.123rf.com/stock-photo/chakra.html"),
    ("india", "https://www.123rf.com/stock-photo/chakora.html"),
    ("india", "https://www.123rf.com/stock-photo/chaprasi.html"),
    ("india", "https://www.123rf.com/stock-photo/dhoti.html"),
    ("india", "https://www.123rf.com/stock-photo/Dhum!_Qalandar.html"),
    ("india", "https://www.123rf.com/stock-photo/Dravidian.html"),
    ("india", "https://www.123rf.com/stock-photo/Durga.html"),
    ("india", "https://www.123rf.com/stock-photo/ekka.html"),
    ("india", "https://www.123rf.com/stock-photo/Farangi.html"),
    ("india", "https://www.123rf.com/stock-photo/Gautama.html"),
    ("india", "https://www.123rf.com/stock-photo/ghazal.html"),
    ("india", "https://www.123rf.com/stock-photo/godown.html"),
    ("india", "https://www.123rf.com/stock-photo/Gopi.html"),
    ("india", "https://www.123rf.com/stock-photo/Govinda.html"),
    ("india", "https://www.123rf.com/stock-photo/gram.html"),
    ("india", "https://www.123rf.com/stock-photo/Gupta.html"),
    ("india", "https://www.123rf.com/stock-photo/Hari.html"),
    ("india", "https://www.123rf.com/stock-photo/hookah.html"),
    ("india", "https://www.123rf.com/stock-photo/jagirdar.html"),
    ("india", "https://www.123rf.com/stock-photo/Jainism.html"),
    ("india", "https://www.123rf.com/stock-photo/khaddar.html"),
    ("india", "https://www.123rf.com/stock-photo/korakora.html"),
    ("india", "https://www.123rf.com/stock-photo/kotha.html"),
    ("india", "https://www.123rf.com/stock-photo/Krishna.html"),
    ("india", "https://www.123rf.com/stock-photo/Kwaja.html"),
    ("india", "https://www.123rf.com/stock-photo/maibi.html"),
    ("india", "https://www.123rf.com/stock-photo/mandal.html"),
    ("india", "https://www.123rf.com/stock-photo/Memsahib.html"),
    ("india", "https://www.123rf.com/stock-photo/Mir.html"),
    ("india", "https://www.123rf.com/stock-photo/Mogul.html"),
    ("india", "https://www.123rf.com/stock-photo/mohallah.html"),
    ("india", "https://www.123rf.com/stock-photo/mudras.html"),
    ("india", "https://www.123rf.com/stock-photo/mushaira.html"),
    ("india", "https://www.123rf.com/stock-photo/Muslim.html"),
    ("india", "https://www.123rf.com/stock-photo/nattuvanar.html"),
    ("india", "https://www.123rf.com/stock-photo/paan.html"),
    ("india", "https://www.123rf.com/stock-photo/pandit.html"),
    ("india", "https://www.123rf.com/stock-photo/punch.html"),
    ("india", "https://www.123rf.com/stock-photo/pariah.html"),
    ("india", "https://www.123rf.com/stock-photo/Parvati.html"),
    ("india", "https://www.123rf.com/stock-photo/pice.html"),
    ("india", "https://www.123rf.com/stock-photo/Radha.html"),
    ("india", "https://www.123rf.com/stock-photo/raga.html"),
    ("india", "https://www.123rf.com/stock-photo/Rama.html"),
    ("india", "https://www.123rf.com/stock-photo/rani.html"),
    ("india", "https://www.123rf.com/stock-photo/rebab.html"),
    ("india", "https://www.123rf.com/stock-photo/rupee.html"),
    ("india", "https://www.123rf.com/stock-photo/sadhu.html"),
    ("india", "https://www.123rf.com/stock-photo/sahukar.html"),
    ("india", "https://www.123rf.com/stock-photo/Sanchi.html"),
    ("india", "https://www.123rf.com/stock-photo/sanyasin.html"),
    ("india", "https://www.123rf.com/stock-photo/sari.html"),
    ("india", "https://www.123rf.com/stock-photo/satyagraha.html"),
    ("india", "https://www.123rf.com/stock-photo/shabash.html"),
    ("india", "https://www.123rf.com/stock-photo/sherwani.html"),
    ("india", "https://www.123rf.com/stock-photo/Siddhartha.html"),
    ("india", "https://www.123rf.com/stock-photo/Sikh.html"),
    ("india", "https://www.123rf.com/stock-photo/Siva.html"),
    ("india", "https://www.123rf.com/stock-photo/stupa.html"),
    ("india", "https://www.123rf.com/stock-photo/subedar.html"),
    ("india", "https://www.123rf.com/stock-photo/Sudras.html"),
    ("india", "https://www.123rf.com/stock-photo/suttee.html"),
    ("india", "https://www.123rf.com/stock-photo/Swami.html"),
    ("india", "https://www.123rf.com/stock-photo/tala.html"),
    ("india", "https://www.123rf.com/stock-photo/uchkin.html"),
    ("india", "https://www.123rf.com/stock-photo/Vedas.html"),
    ("india", "https://www.123rf.com/stock-photo/Vedanta.html"),
    ("india", "https://www.123rf.com/stock-photo/Vishnu.html")
]
main(urls)
