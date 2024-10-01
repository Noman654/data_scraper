# Image Scraper for pexels.com

This script scrapes images from specified URLs, stores their metadata in MongoDB, and uploads the images to AWS S3. The scraping is performed using Selenium in headless mode, and the images are downloaded and processed in parallel using multiprocessing and multithreading.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.6 or later
- MongoDB installed and running
- AWS account with S3 bucket configured
- ChromeDriver installed and added to your PATH

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/ola-silicon/data-scraper-all.git
    cd your-repository
    ```

2. **Create and activate a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3. **Configure AWS credentials:**
    Ensure you have configured your AWS CLI with the necessary credentials:
    ```bash
    aws configure
    ```

## Configuration

- **AWS S3 Bucket:** Update the `BUCKET_NAME` variable in the script with your S3 bucket name.
- **MongoDB:** Ensure MongoDB is running locally or update the connection string if using a remote MongoDB instance.
- **ChromeDriver:** Ensure the `Service` path in the script points to the correct location of your ChromeDriver.

## Usage

To run the scraper, execute the `main` function with the desired URLs:

```bash
python pixeldownload.py
```
## Code Overview

### Main Functions

- **`main(urls)`**: Main function to handle multiple URLs.
- **`process_url(folder_name, url, image_count, lock)`**: Processes a single URL, downloads images, and stores metadata.

### Helper Functions

- **`store_image_metadata(folder_name, img_url, s3_key, img_name, alt_text)`**: Stores image metadata in MongoDB.
- **`download_image(img_info, folder_name, lock)`**: Downloads images with retry mechanism.
- **`upload_images_to_s3_and_delete_local(image_dir, folder_name)`**: Uploads images to S3 and deletes them locally.
- **`get_s3_folder_stats(folder_name)`**: Gets S3 folder size and image count.

### Debugging

In headless mode, the script captures screenshots and logs page sources to help with debugging. These files are saved in the current directory for each folder processed:

- `folder_name_debug_screenshot.png`
- `folder_name_debug_page_source.html`

### Logging

Logs are saved to `scraping_log.log` and are also printed to the console. The logging captures important events and errors during the scraping process.


