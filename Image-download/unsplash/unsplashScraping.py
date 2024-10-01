import os
import csv
import urllib.request
from playwright.sync_api import sync_playwright
import hashlib
import boto3
import logging
import time

# AWS S3 setup
s3 = boto3.client('s3')
BUCKET_NAME = 'voice-annotation-testing'

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

def store_image_metadata_to_csv(csv_file, folder_name, img_url, s3_key, img_name, alt_text):
    with open(csv_file, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([folder_name, img_url, s3_key, img_name, alt_text])
    logging.info(f"Stored metadata for image: {img_name} in CSV")

def upload_image_to_s3(image_path, folder_name, image_name):
    s3_key = f"Audio-video-image-dataset/unsplash-images-download/{folder_name}/{image_name}"
    try:
        s3.upload_file(image_path, BUCKET_NAME, s3_key)
        os.remove(image_path)
        logging.info(f"Uploaded and removed {image_name} from local storage.")
        return s3_key
    except Exception as e:
        logging.error(f"Failed to upload {image_name} to S3: {e}")
        return None

def download_image(url, output_dir, image_name):
    try:
        # Download the image and save it to the output directory
        urllib.request.urlretrieve(url, os.path.join(output_dir, image_name))
        logging.info(f"Downloaded {image_name}")
    except Exception as e:
        logging.error(f"Failed to download {image_name}: {e}")

def fetch_and_download_images(search_term, output_dir, csv_file, max_images=1000):
    base_url = f"https://unsplash.com/s/photos/{search_term}"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        downloaded_images = 0
        current_page = 1
        
        while downloaded_images < max_images:
            page.goto(f"{base_url}?page={current_page}")
            page.wait_for_load_state('networkidle')
            images = page.query_selector_all('img')

            if not images:
                break

            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            for img in images:
                if downloaded_images >= max_images:
                    break

                image_url = img.get_attribute('src')

                if image_url:
                    
                    meta_data = f"image_{downloaded_images + 1}"
                    unique_id = hashlib.md5(image_url.encode()).hexdigest()
                    image_name = f"{meta_data}_{unique_id}.jpg"
                    if image_url.startswith('/'):
                        image_url = page.url.split('/')[0] + '//' + page.url.split('/')[2] + image_url
                    
                    download_image(image_url, output_dir, image_name)
                    
                    s3_key = upload_image_to_s3(os.path.join(output_dir, image_name), search_term, image_name)
                    if s3_key:
                        alt_text = img.get_attribute('alt')
                        store_image_metadata_to_csv(csv_file, search_term, image_url, s3_key, image_name, alt_text)
                    
                    downloaded_images += 1

            current_page += 1
            time.sleep(2)  # Optional: Pause to prevent too many requests in a short period

        logging.info(f"All images downloaded to '{output_dir}' and metadata saved to '{csv_file}'")
        browser.close()

def scraper_for_all_terms(terms, max_images_per_term=1000):
    for term in terms:
        logging.info(f"Scraping for term: {term}")
        path = f"{term}"
        os.makedirs(path, exist_ok=True)
        csv_file = f"{term}_metadata.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['folder_name', 'img_url', 's3_key', 'img_name', 'alt_text'])
        fetch_and_download_images(term, path, csv_file, max_images=max_images_per_term)
        time.sleep(60)

    logging.info("All terms completed.")

if __name__ == "__main__":
    search_terms = [
        "India",
        "Indian culture",
        "Indian food",
        "Indian recipes",
        "Indian cuisine",
        "Indian street food",
        "Indian fashion",
        "Indian clothing",
        "Indian saree",
        "Indian jewelry",
        "Indian bridal wear",
        "Indian weddings",
        "Indian traditions",
        "Indian festivals",
        "Diwali",
        "Holi",
        "Indian art",
        "Indian crafts",
        "Indian decor",
        "Indian home decor",
        "Indian architecture",
        "Indian travel",
        "Indian destinations",
        "Taj Mahal",
        "Indian landscapes",
        "Indian history",
        "Indian heritage",
        "Indian temples",
        "Indian spirituality",
        "Ayurveda",
        "Indian movies",
        "Bollywood",
        "Indian music",
        "Indian dance",
        "Indian classical dance",
        "Kathak",
        "Bharatanatyam",
        "Indian celebrities",
        "Indian literature",
        "Indian books",
        "Indian authors",
        "Indian mythology",
        "Indian festivals",
        "Indian rangoli",
        "Indian mehndi",
        "Indian henna",
        "Indian beauty",
        "Indian skincare",
        "Indian hairstyles",
        "Indian makeup",
        "Indian spices",
        "Indian chai",
        "Indian tea",
        "Indian sweets",
        "Indian desserts",
        "Indian street markets",
        "Indian textiles",
        "Indian embroidery",
        "Indian block print",
        "Indian painting",
        "Indian sculptures",
        "Indian pottery",
        "Indian furniture",
        "Indian rugs",
        "Indian carpets",
        "Indian interior design",
        "Indian festivals",
        "Ganesh Chaturthi",
        "Navratri",
        "Durga Puja",
        "Pongal",
        "Eid in India",
        "Christmas in India",
        "Indian wildlife",
        "Indian national parks",
        "Indian tigers",
        "Indian elephants",
        "Indian spices",
        "Indian cooking",
        "Indian street photography",
        "Indian cities",
        "Mumbai",
        "Delhi",
        "Bangalore",
        "Kolkata",
        "Chennai",
        "Hyderabad",
        "Goa",
        "Kerala",
        "Rajasthan",
        "Jaipur",
        "Udaipur",
        "Jodhpur",
        "Indian forts",
        "Indian palaces",
        "Indian markets",
        "Indian souvenirs",
        "Indian festivals of lights",
        "Indian folk art",
        "Indian tribal art",
        "Indian fabrics",
        "Indian dresses",
        "Indian tops",
        "Indian salwar kameez",
        "Indian kurtis",
        "Indian lehenga",
        "Indian shawls",
        "Indian scarves",
        "Indian weddings ideas",
        "Indian bridal jewelry",
        "Indian wedding decor",
        "Indian marriage customs",
        "Indian wedding invitations",
        "Indian wedding traditions",
        "Indian honeymoon destinations",
        "Indian sports",
        "Cricket in India",
        "Indian cricket team",
        "Indian athletes",
        "Indian festivals decoration",
        "Indian lanterns",
        "Indian tapestries",
        "Indian boho decor",
        "Indian bohemian style",
        "Indian fusion recipes",
        "Indian travel tips",
        "Indian vacation ideas",
        "Indian beach destinations",
        "Indian mountains",
        "Indian trekking",
        "Indian adventures",
        "Indian rural life",
        "Indian farmers",
        "Indian agriculture",
        "Indian independence day",
        "Indian republic day",
        "Indian patriotic",
        "Indian freedom fighters",
        "Indian republic day parade",
        "Indian army",
        "Indian navy",
        "Indian air force",
        "Indian defense"
    ]
    
    max_images_per_term = int(input("Enter the maximum number of images to download per term (or 0 for all available): "))
    max_images_per_term = max_images_per_term if max_images_per_term > 0 else float('inf')
    scraper_for_all_terms(search_terms, max_images_per_term)
