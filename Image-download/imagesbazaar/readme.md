# Image Download from Image-Bazaar

This scripts is designed to scrape images from a specified URL using Selenium and BeautifulSoup, download these images locally, and then upload them to an AWS S3 bucket. The script also removes the local copies of the images after successfully uploading them to S3. Logging is set up to record the process and any errors that may occur.

## Prerequisites

1. **Python 3.7+**
2. **Google Chrome**
3. **Chromedriver**: Ensure you have the correct version of Chromedriver that matches your Chrome version.
4. **AWS CLI**: Configured with the necessary credentials to access your S3 bucket.
5. **Python Packages**: Install the required packages using the following command:
    ```sh
    pip install boto3 selenium requests beautifulsoup4
    ```

    
## Setup

1. **Install Chromedriver**: 
   - Download Chromedriver from the [official site](https://sites.google.com/a/chromium.org/chromedriver/downloads).
   - Place the Chromedriver executable in a known location (`/home/mdalam8/beautifulsetup/chromedriver-linux64/chromedriver` in this case).

2. **AWS Configuration**:
   - Configure AWS CLI with your credentials:
     ```sh
     aws configure
     ```

## Script Overview

### AWS S3 Setup

The script uses `boto3` to interact with the AWS S3 service. Make sure to set the `BUCKET_NAME` variable to your target S3 bucket name.

### Logging

Logging is set up to record information to both the console and a file named `scraping_log.log`.

### Image Download with Retry Mechanism

The `download_image` function downloads an image with a retry mechanism to handle temporary issues like network problems or server errors.

### S3 Upload and Local Deletion

The `upload_images_to_s3_and_delete_local` function uploads images to the S3 bucket and then deletes them locally to free up space.

### URL Processing

The `process_url` function:
- Initializes a headless Chrome browser.
- Opens the specified URL.
- Scrolls the page to load images dynamically.
- Parses the page with BeautifulSoup.
- Downloads images using multithreading.
- Periodically uploads images to S3.
- Ensures all images are uploaded before finishing.

### Main Function

The `main` function manages multiple URLs using a process pool. Each URL is processed in a separate process to handle large-scale scraping efficiently.

## Usage

1. **Define URLs**:
   - Edit the `urls` list in the `main` function with your target folder names and URLs.

2. **Run the Script**:
   ```sh
   python scraping_script.py
