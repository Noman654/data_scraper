# Pixabay Image Scraper and Uploader

## Overview
This Python script allows you to scrape images from Pixabay based on specified keywords, download them locally, and upload them to AWS S3. It uses Selenium for web scraping and Boto3 for interacting with AWS S3.

## Installation

### Prerequisites
- Python 3.x installed on your system
- Chrome browser installed

### WebDriver
You need to download the ChromeDriver executable compatible with your Chrome browser version and place it in your system's PATH. 


## Configuration

### MongoDB
Ensure you have MongoDB installed or access to a MongoDB instance. Configure the MongoDB connection string and database details in the script (scrapPixabay.py):


##### MONGO_CONNECTION_STRING 
##### MONGO_DB_NAME 
##### MONGO_COLLECTION_NAME 
##### AWS S3

### Configure your AWS credentials and create an S3 bucket
#### Set the bucket name (S3_BUCKET_NAME) and prefix (S3_PREFIX) where you want to upload images in the script


### Install Dependencies
Install the required Python libraries using pip:
```bash
pip install selenium beautifulsoup4 tqdm pymongo requests boto3
