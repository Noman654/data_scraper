# Concurrent Snapshot Processing of HuggingFace with Multiprocessing

## Overview

This Python script downloads multiple snapshots concurrently from a source repository using `huggingface_hub` and upload them to Amazon S3. It utilizes Python's `multiprocessing` module to leverage multiple CPU cores for parallel processing, optimizing the download and upload speed of large datasets.

### Features:
- **Concurrency:** Utilizes all available CPU cores to process multiple snapshots concurrently.
- **Logging:** Tracks the progress and status of each snapshot and file processing operation using Python's logging module.
- **Error Handling:** Logs errors and exceptions encountered during download and upload operations.
- **Database Integration:** Stores processing metadata in MongoDB for tracking and auditing purposes.

## Requirements

Ensure you have the following installed:
- Python 3.x
- Dependencies listed in `requirements.txt` (install via `pip install -r requirements.txt`)
  - boto3
  - huggingface_hub
  - pymongo

## Setup

1. **Clone the Repository:**
   ```bash
   git clone <git@github.com:ola-silicon/data-scraper-all.git>
   cd <repository_directory>

## Configure Environment Variables:

  - Set up AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) with permissions to access the target S3 bucket.
  - Ensure MongoDB server is configured on the machine.
  
## Adjust Configuration:

  - Update dataset_source and snapshots in main() function according to your dataset and snapshot names.
  - Customize region_name, bucket_name, and s3_key_prefix for your specific S3 bucket configuration.

## Logging

- Logs are outputted to the console with timestamps indicating the progress of snapshot and file processing.
- Critical errors are logged with stack traces for debugging purposes.
