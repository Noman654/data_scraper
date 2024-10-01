
import os
import time
import requests
import boto3
import logging
from multiprocessing import Pool
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient
from datetime import datetime

# AWS S3 setup
s3 = boto3.client('s3')
BUCKET_NAME = 'voice-annotation-testing'  # Correct bucket name

# MongoDB setup
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

# Function to upload images to S3 and delete them locally
def upload_images_to_s3_and_delete_local(image_dir, folder_name):
    for img_name in os.listdir(image_dir):
        img_path = os.path.join(image_dir, img_name)
        s3_key = f"Audio-video-image-dataset/images-june-download/getty-images/{folder_name}/{img_name}"  # Correct key
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
    prefix = f"Audio-video-image-dataset/images-june-download/getty-images/{folder_name}/"
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            total_size += obj['Size']
            total_count += 1
    
    return total_size, total_count

# Function to store folder metadata in MongoDB
def store_folder_metadata(folder_name, start_time, end_time):
    total_size, total_count = get_s3_folder_stats(folder_name)
    metadata = {
        'folder_name': folder_name,
        'start_time': start_time,
        'end_time': end_time,
        'total_images': total_count,
        'total_size': total_size
    }
    try:
        collection.update_one(
            {'folder_name': folder_name},
            {'$set': metadata},
            upsert=True
        )
        logging.info(f"Stored metadata for folder: {folder_name}")
    except Exception as e:
        logging.error(f"Failed to store metadata for folder {folder_name}: {e}")

# Set up Selenium WebDriver
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    service = Service('/home/mdalam8/beautifulsetup/chromedriver-linux64/chromedriver')  # Update with your chromedriver path
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

# List of names
names = [
    "India", "Indian culture", "Indian food", "Indian recipes", "Indian cuisine",
    "Indian street food", "Indian fashion", "Indian clothing", "Indian saree", 
    "Indian jewelry", "Indian bridal wear", "Indian weddings", "Indian traditions", 
    "Indian festivals", "Diwali", "Holi", "Indian art", "Indian crafts", "Indian decor",
    "Indian home decor", "Indian architecture", "Indian travel", "Indian destinations", 
    "Taj Mahal", "Indian landscapes", "Indian history", "Indian heritage", "Indian temples", 
    "Indian spirituality", "Ayurveda", "Indian movies", "Bollywood", "Indian music", 
    "Indian dance", "Indian classical dance", "Kathak", "Bharatanatyam", "Indian celebrities",
    "Indian literature", "Indian books", "Indian authors", "Indian mythology", "Indian festivals",
    "Indian rangoli", "Indian mehndi", "Indian henna", "Indian beauty", "Indian skincare",
    "Indian hairstyles", "Indian makeup", "Indian spices", "Indian chai", "Indian tea",
    "Indian sweets", "Indian desserts", "Indian street markets", "Indian textiles",
    "Indian embroidery", "Indian block print", "Indian painting", "Indian sculptures",
    "Indian pottery", "Indian furniture", "Indian rugs", "Indian carpets",
    "Indian interior design", "Indian festivals", "Ganesh Chaturthi", "Navratri", "Durga Puja",
    "Pongal", "Eid in India", "Christmas in India", "Indian wildlife", "Indian national parks",
    "Indian tigers", "Indian elephants", "Indian spices", "Indian cooking",
    "Indian street photography", "Indian cities", "Mumbai", "Delhi", "Bangalore", "Kolkata",
    "Chennai", "Hyderabad", "Goa", "Kerala", "Rajasthan", "Jaipur", "Udaipur", "Jodhpur",
    "Indian forts", "Indian palaces", "Indian markets", "Indian souvenirs", "Indian festivals of lights",
    "Indian folk art", "Indian tribal art", "Indian fabrics", "Indian dresses", "Indian tops",
    "Indian salwar kameez", "Indian kurtis", "Indian lehenga", "Indian shawls", "Indian scarves",
    "Indian weddings ideas", "Indian bridal jewelry", "Indian wedding decor", "Indian marriage customs",
    "Indian wedding invitations", "Indian wedding traditions", "Indian honeymoon destinations",
    "Indian sports", "Cricket in India", "Indian cricket team", "Indian athletes",
    "Indian festivals decoration", "Indian lanterns", "Indian tapestries", "Indian boho decor",
    "Indian bohemian style", "Indian fusion recipes", "Indian travel tips", "Indian vacation ideas",
    "Indian beach destinations", "Indian mountains", "Indian trekking", "Indian adventures",
    "Indian rural life", "Indian farmers", "Indian agriculture", "Indian independence day",
    "Indian republic day", "Indian patriotic", "Indian freedom fighters", "Indian republic day parade",
    "Indian army", "Indian navy", "Indian air force", "Indian defense"
]  # Add more names as needed

# Function to download an image
def download_image(img_url, folder_name, img_name):
    try:
        # Print the original URL
        print(f"Original URL for {img_name}: {img_url}")
        # Modify the URL to have w=0 if w=gi exists
        if 'w=gi' in img_url:
            img_url = img_url.replace("w=gi", "w=0")
        # Print the modified URL
        print(f"Modified URL for {img_name}: {img_url}")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(img_url, headers=headers, stream=True)
        
        if response.status_code == 200:
            with open(os.path.join(folder_name, img_name), 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
        else:
            print(f"Failed to download {img_url}: Status code {response.status_code}")
    except Exception as e:
        print(f"Failed to download {img_url}: {e}")

# Function to scroll and load images
def scroll_and_load_images(driver, folder_name, image_count):
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # Find all image elements
    images = soup.find_all('img', class_='BLA_wBUJrga_SkfJ8won')
    
    # Download images
    for img in images:
        img_url = img.get('src')
        if img_url and img_url.startswith('http'):
            img_name = f"image_{image_count}.jpg"
            download_image(img_url, folder_name, img_name)
            image_count += 1
        else:
            print(f"Skipping invalid URL: {img_url}")
    
    return image_count, len(images)

# Function to process a single name
def process_name(name):
    driver = setup_driver()
    search_url = f"https://www.gettyimages.in/search/2/image?phrase={name.replace(' ', '%20')}&sort=best&license=rf%2Crm"
    folder_name = name.replace(" ", "_")
    
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    # Record start time
    start_time = datetime.now()
    
    try:
        # Open the website
        driver.get(search_url)

        # Wait for the page to load
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'img')))
        
        # Initialize image counter
        image_count = 0
        
        # Loop through pages and scrape images
        while True:
            # Scroll and load images on the current page
            image_count, images_loaded = scroll_and_load_images(driver, folder_name, image_count)
            
            if images_loaded == 0:
                break  # Break if no images were loaded

            # Check if there is a next page button
            try:
                next_button = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.CLASS_NAME, 'Npj3TMjwvq4A76qbyQTN')))
                next_button.click()
                # Wait for the next page to load
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'img')))
            except (NoSuchElementException, TimeoutException):
                break  # Break if there is no next page button or timeout occurs

    except Exception as e:
        logging.error(f"Error processing {name}: {e}")
    finally:
        # Close the browser
        driver.quit()

        # Record end time
        end_time = datetime.now()

        # Upload remaining images to S3 and delete locally
        upload_images_to_s3_and_delete_local(folder_name, folder_name)

        # Store metadata in MongoDB
        store_folder_metadata(folder_name, start_time, end_time)

        # Remove local folder
        try:
            if os.path.exists(folder_name):
                os.rmdir(folder_name)
        except OSError as e:
            logging.error(f"Failed to remove folder {folder_name}: {e}")

# Wrapper function for error handling
def process_with_error_handling(name):
    try:
        process_name(name)
    except Exception as e:
        logging.error(f"Error in processing {name}: {e}")

# Main function to handle multiprocessing
def main():
    with Pool(processes=min(16, len(names))) as pool:
        pool.map(process_with_error_handling, names)

if __name__ == "__main__":
    main()
