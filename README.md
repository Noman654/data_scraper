# Data Scraper and Downloader

## Overview
This repository contains multiple Python scripts designed for scraping and downloading data from various sources, including GitHub repositories, Hugging Face datasets, and other online resources. The scripts utilize various libraries for efficient data handling, including `requests`, `boto3`, `pymongo`, and `selenium`.

## Features
- **GitHub Scraper**: Collects repository information such as name, star count, and primary language.
- **Hugging Face Dataset Downloader**: Downloads datasets concurrently using multiprocessing.
- **Archive Downloader**: Downloads files from the Internet Archive and uploads them to Amazon S3.
- **Getty Images Downloader**: Scrapes images from Getty Images and uploads them to S3.
- **Error Handling**: Each script includes error handling to manage common issues like API rate limits and connection errors.
- **Logging**: Progress and errors are logged for easier debugging and tracking.

## Requirements
Ensure you have the following installed:
- Python 3.6+
- Required libraries (install via `pip install -r requirements.txt`):
  - requests
  - boto3
  - pymongo
  - selenium
  - tqdm
  - PyYAML
  - aioboto3
  - beautifulsoup4

## Installation
1. **Clone this repository**:
   ```bash
   git clone https://github.com/ola-silicon/data-scraper-all.git
   cd data-scraper-all
   ```

2. **Install the required dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
Create a `config.yaml` file in the project root with the following structure:
