import os
import time
import requests
import boto3
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from multiprocessing import Pool, Manager
from threading import Thread
from random import randint
from time import sleep

# AWS S3 setup
s3 = boto3.client('s3')
BUCKET_NAME = 'voice-annotation-testing'  # Correct bucket name

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraping_log.log"),
        logging.StreamHandler()
    ]
)

# Function to download image with retry mechanism
def download_image(img_info):
    img_url, img_name, image_dir = img_info
    retries = 5
    for i in range(retries):
        try:
            with requests.Session() as session:
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                })
                response = session.get(img_url, stream=True, timeout=10)
                response.raise_for_status()  # Raise an error for bad status codes
                with open(os.path.join(image_dir, img_name), 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
            logging.info(f"Downloaded image: {img_name}")
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
        s3_key = f"Audio-video-image-dataset/images-june-download/imagebazzar/{folder_name}/{img_name}"  # Correct key
        try:
            s3.upload_file(img_path, BUCKET_NAME, s3_key)
            os.remove(img_path)
            logging.info(f"Uploaded and removed {img_name} from local storage.")
        except Exception as e:
            logging.error(f"Failed to upload {img_name} to S3: {e}")

# Function to process a single URL
def process_url(folder_name, url, image_count, lock):
    logging.info(f"Processing folder: {folder_name} with URL: {url}")

    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = Service('/home/mdalam8/beautifulsetup/chromedriver-linux64/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Open the URL
    driver.get(url)
    time.sleep(5)

    # Create a specific directory for the current link if it doesn't exist
    image_dir = os.path.join("images", folder_name)
    if not os.path.exists(image_dir):
        os.makedirs(image_dir)

    # Initialize variables for infinite scroll
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Scroll to the bottom of the page
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Wait for new images to load
        time.sleep(5)
        
        # Parse the page source with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find all image tags
        images = soup.find_all('img')
        
        # Loop through images and download them
        img_info_list = []
        for img in images:
            img_url = img.get('src') or img.get('data-src')
            if img_url and img_url.startswith('http'):
                alt_text = img.get('alt', '').replace(' ', '_').replace('/', '_')
                img_name = f"image_{image_count.value}_{alt_text}.jpg"
                img_info_list.append((img_url, img_name, image_dir))
                with lock:
                    image_count.value += 1

                # Upload images to S3 and delete locally after every 100 images
                if image_count.value % 100 == 0:
                    logging.info(f"Uploading batch of 100 images to S3 for folder: {folder_name}")
                    upload_images_to_s3_and_delete_local(image_dir, folder_name)
            else:
                logging.warning(f"Skipping invalid URL: {img_url}")
        
        # Download images using threads
        threads = []
        for img_info in img_info_list:
            thread = Thread(target=download_image, args=(img_info,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
        
        # Check if we have reached the end of the page
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Upload remaining images to S3 and delete locally
    logging.info(f"Uploading remaining images to S3 for folder: {folder_name}")
    upload_images_to_s3_and_delete_local(image_dir, folder_name)

    # Quit the driver
    driver.quit()

# Main function to handle multiple URLs
def main(urls):
    manager = Manager()
    lock = manager.Lock()
    
    # Create a process pool
    with Pool() as pool:
        pool.starmap(process_url, [(folder_name, url, manager.Value('i', 0), lock) for folder_name, url in urls])

if __name__ == "__main__":
    urls = [
         ("IndianCulture", "https://www.imagesbazaar.com/search/color%20Indianculture?displayKeyword=6a4d474a424d0360564f57565146031d1d03754a465403624f4f&br=1") 
           ]
    main(urls)


#/home/mdalam8/beautifulsetup/chromedriver-linux64/chromedriver
