import os
import time
import requests
import boto3
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from multiprocessing import Pool, Manager
from threading import Thread
from random import randint
from pymongo import MongoClient
from datetime import datetime

# AWS S3 setup
s3 = boto3.client('s3')
BUCKET_NAME = 'voice-annotation-testing'  # Correct bucket name

# MongoDB setup
# client = MongoClient('localhost', 27017)  # Adjust the host and port if needed
# db = client['image_metadata_db']
# collection = db['video_metadata']
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

# Function to store video metadata in MongoDB
def store_video_metadata(folder_name, video_url, s3_key, video_name, alt_text):
    metadata = {
        'video_url': video_url,
        's3_key': s3_key,
        'video_name': video_name,
        'alt_text': alt_text
    }
    try:
        collection.update_one(
            {'folder_name': folder_name},
            {'$push': {'videos': metadata}},
            upsert=True
        )
        logging.info(f"Stored metadata for video: {video_name}")
    except Exception as e:
        logging.error(f"Failed to store metadata for {video_name}: {e}")

# Function to download video with retry mechanism
def download_video(video_info, folder_name, lock):
    video_url, video_name, video_dir = video_info
    retries = 5
    for i in range(retries):
        try:
            with requests.Session() as session:
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                })
                response = session.get(video_url, stream=True, timeout=10)
                response.raise_for_status()  # Raise an error for bad status codes
                with open(os.path.join(video_dir, video_name), 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
            logging.info(f"Downloaded video: {video_name}")

            # Store metadata in MongoDB
            s3_key = f"Audio-video-image-dataset/videos-june-download/pixel/{folder_name}/{video_name}"
            store_video_metadata(folder_name, video_url, s3_key, video_name, os.path.splitext(video_name)[0])
            break  # Exit the retry loop if successful
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading {video_url}: {e}")
            time.sleep(2 ** i + randint(0, 1000) / 1000)  # Exponential backoff
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            break

# Function to upload videos to S3 and delete them locally
def upload_videos_to_s3_and_delete_local(video_dir, folder_name):
    for video_name in os.listdir(video_dir):
        video_path = os.path.join(video_dir, video_name)
        s3_key = f"Audio-video-image-dataset/videos-june-download/pixel/{folder_name}/{video_name}"  # Correct key
        try:
            s3.upload_file(video_path, BUCKET_NAME, s3_key)
            os.remove(video_path)
            logging.info(f"Uploaded and removed {video_name} from local storage.")
        except Exception as e:
            logging.error(f"Failed to upload {video_name} to S3: {e}")

# Function to get S3 folder size and video count
def get_s3_folder_stats(folder_name):
    total_size = 0
    total_count = 0
    prefix = f"Audio-video-image-dataset/videos-june-download/pixel/{folder_name}/"
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            total_size += obj['Size']
            total_count += 1
    
    return total_size, total_count

# Function to process a single URL
def process_url(folder_name, url, video_count, lock):
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
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    service = Service('/home/mdalam8/beautifulsetup/chromedriver-linux64/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Open the URL
    driver.get(url)

    # Explicit wait to ensure page is loaded
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "video"))
        )
    except Exception as e:
        logging.error(f"Error waiting for videos to load: {e}")

    # Capture screenshot for debugging
    driver.save_screenshot(f'{folder_name}_debug_screenshot.png')
    logging.info(f"Saved screenshot for debugging: {folder_name}_debug_screenshot.png")

    # Log the page source for debugging
    page_source = driver.page_source
    with open(f'{folder_name}_debug_page_source.html', 'w', encoding='utf-8') as f:
        f.write(page_source)
    logging.info(f"Saved page source for debugging: {folder_name}_debug_page_source.html")

    # Create a specific directory for the current link if it doesn't exist
    video_dir = os.path.join("videos", folder_name)
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)

    # Initialize variables for infinite scroll
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Scroll to the bottom of the page
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Wait for new videos to load
        time.sleep(5)
        
        # Parse the page source with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find all video tags
        videos = soup.find_all('video', class_='VideoTag_video__LcmT4')
        
        # Loop through videos and download them
        video_info_list = []
        for video in videos:
            video_url = video.get('src')
            if video_url and video_url.startswith('http'):
                alt_text = video.get('alt', '').replace(' ', '_').replace('/', '_')
                video_name = f"video_{video_count.value}_{alt_text}.mp4"
                video_info_list.append((video_url, video_name, video_dir))
                with lock:
                    video_count.value += 1

                # Upload videos to S3 and delete locally after every 100 videos
                if video_count.value % 100 == 0:
                    logging.info(f"Uploading batch of 100 videos to S3 for folder: {folder_name}")
                    upload_videos_to_s3_and_delete_local(video_dir, folder_name)
            else:
                logging.warning(f"Skipping invalid URL: {video_url}")
        
        # Download videos using threads
        threads = []
        for video_info in video_info_list:
            thread = Thread(target=download_video, args=(video_info, folder_name, lock))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
        
        # Check if we have reached the end of the page
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Upload remaining videos to S3 and delete locally
    logging.info(f"Uploading remaining videos to S3 for folder: {folder_name}")
    upload_videos_to_s3_and_delete_local(video_dir, folder_name)

    # Quit the driver
    driver.quit()

    end_time = datetime.now()

    # Get total size and count of videos from S3
    total_size, total_count = get_s3_folder_stats(folder_name)

    # Store folder metadata
    try:
        collection.update_one(
            {'folder_name': folder_name},
            {'$set': {
                'total_videos': total_count,
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
    manager = Manager()
    lock = manager.Lock()
    
    # Create a process pool
    with Pool() as pool:
        pool.starmap(process_url, [(folder_name, url, manager.Value('i', 0), lock) for folder_name, url in urls])

if __name__ == "__main__":
    urls = [
    ("indian-festival", "https://www.pexels.com/search/videos/indian%20festival/"),
    ("indian-food", "https://www.pexels.com/search/videos/indian%20food/"),
    ("indian-culture", "https://www.pexels.com/search/videos/indian%20culture/"),
    ("indian-travel", "https://www.pexels.com/search/videos/indian%20travel/"),
    ("indian-wedding", "https://www.pexels.com/search/videos/indian%20wedding/"), 
    ("india", "https://www.pexels.com/search/videos/india/"),
    ("indian-architecture", "https://www.pexels.com/search/videos/indian%20architecture/"),
    ("indian-nature", "https://www.pexels.com/search/videos/indian%20nature/"),
    ("indian-streets", "https://www.pexels.com/search/videos/indian%20streets/"),
    ("indian-fashion", "https://www.pexels.com/search/videos/indian%20fashion/"),
    ("indian-people", "https://www.pexels.com/search/videos/indian%20people/"),
    ("indian-landscapes", "https://www.pexels.com/search/videos/indian%20landscapes/"),
    ("indian-wildlife", "https://www.pexels.com/search/videos/indian%20wildlife/"),
    ("indian-music", "https://www.pexels.com/search/videos/indian%20music/"),
    ("indian-dance", "https://www.pexels.com/search/videos/indian%20dance/"),
    ("indian-art", "https://www.pexels.com/search/videos/indian%20art/"),
    ("indian-temples", "https://www.pexels.com/search/videos/indian%20temples/"),
    ("indian-markets", "https://www.pexels.com/search/videos/indian%20markets/"),
    ("indian-crafts", "https://www.pexels.com/search/videos/indian%20crafts/"),
    ("indian-street-food", "https://www.pexels.com/search/videos/indian%20street%20food/"),
    ("indian-monuments", "https://www.pexels.com/search/videos/indian%20monuments/"),
    ("indian-rituals", "https://www.pexels.com/search/videos/indian%20rituals/"),
    ("indian-festivals-at-night", "https://www.pexels.com/search/videos/indian%20festivals%20night/"),
    ("indian-countryside", "https://www.pexels.com/search/videos/indian%20countryside/"),
    ("indian-beach", "https://www.pexels.com/search/videos/indian%20beach/"),
    ("indian-rivers", "https://www.pexels.com/search/videos/indian%20rivers/")
]
    
    main(urls)
