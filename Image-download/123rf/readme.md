# Image Scraper for 123rf

This script scrapes images from 123rf, saves them locally, uploads them to an AWS S3 bucket, and store metadata in a MongoDB database.

## Features

- Scrapes images from a specified URL using Selenium and BeautifulSoup.
- Downloads images locally.
- Uploads images to an AWS S3 bucket.
- Stores image metadata (URL, S3 key, image name, alt text) in a MongoDB database.
- Handles rate limiting and retries for image downloading.

## Prerequisites

- Python 3.x
- Google Chrome browser
- ChromeDriver
- MongoDB
- AWS account with S3 bucket
- AWS CLI configured with appropriate permissions

## Installation

1. **Clone the repository**:
    ```bash
    git clone https://github.com/ola-silicon/data-scraper-all/.git

2. **Download ChromeDriver**:
    - Ensure you have the correct version of ChromeDriver that matches your Chrome browser version.
    - Download from [ChromeDriver](https://sites.google.com/a/chromium.org/chromedriver/downloads).
    - Place the ChromeDriver executable in a known directory and update the path in the script if necessary.

3. **Configure AWS CLI**:
    ```bash
    aws configure
    ```
    - Enter your AWS Access Key ID, Secret Access Key, region, and output format.

4. **Start MongoDB**:
    - Ensure MongoDB is installed and running on your system.

## Configuration

Update the following parameters in the script as needed:

- **AWS S3 bucket name**:
    ```python
    BUCKET_NAME = 'voice-annotation-testing'
    ```

- **ChromeDriver path**:
    ```python
    service = Service('/path/to/your/chromedriver')
    ```

- **MongoDB connection**:
    ```python
    client = MongoClient('localhost', 27017)
    ```

## Usage

1. **Run the script**:
    ```bash
    python image_scraper_uploader.py
    ```

2. **Script Parameters**:
    - The script processes a list of URLs. You can update the `urls` list in the `main` function:
      ```python
      urls = [
           ("india", "https://www.123rf.com/stock-photo/india.html") 
      ]
      ```

3. **Logging**:
    - Logs are stored in `scraping_log.log` and also printed to the console.

